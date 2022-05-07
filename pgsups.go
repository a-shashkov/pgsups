package main

// Замеряем скорость работы разных способов массового UPDATE в PostgreSQL

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"

	"database/sql"
	//_ "github.com/lib/pq"
	_ "github.com/jackc/pgx/v4/stdlib"
)

// строка подключения к БД
const connect = "dbname=MYDB sslmode=disable user=postgres password="

// всего записей в таблице
const totalRecs = 100000

// сколько из них изменяем
const updateRecs = 4000

// сколько вызовов для каждого метода усредняем
const testCount = 20

// такой тип будет у функций обновления
// первый параметр - список изменяемых идентификаторов, второй - для некоторых методов размер "порции"
// результат - описание метода
type fupdate = func([]int32, int) string

func main() {
	//создаём таблицу и заполняем её
	prepareTable()
	//замеряем время работы разных методов
	fmt.Printf("Тестируем изменение %d записей, усредняем по %d тестам \n", updateRecs, testCount)

	bench(updateStupid, 0)
	bench(updatePrepared, 0)
	bench(updateClassic1, 0)

	bench(updateTransactPrep, 0)
	bench(updateTransactPrep4, 0)
	bench(updateTransactPrep8, 0)

	bench(updateTransactTextN, 4)
	bench(updateTransactTextN, 8)
	bench(updateTransactTextN, 25)
	bench(updateTransactTextN, 100)
	bench(updateTransactTextN, 250)

	bench(updateTextN, 4)
	bench(updateTextN, 8)
	bench(updateTextN, 25)
	bench(updateTextN, 100)
	bench(updateTextN, 250)

	bench(updateTempN, 8)
	bench(updateTempN, 25)
	bench(updateTempN, 100)

	//завершаем работу
	done()
}

// глобальные переменные
var db *sql.DB
var gen *rand.Rand
var dur1 float64

//создаём таблицу и заполняем её
func prepareTable() {
	if len(sql.Drivers()) == 0 {
		log.Fatal("не подключен драйвер БД в секции import")
	}
	//подключаемся к БД
	var err error
	s := connect
	if len(os.Args) > 1 {
		s = connect + os.Args[1]
	}
	if db, err = sql.Open(sql.Drivers()[0], s); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Драйвер БД: %s \n", sql.Drivers()[0])
	//создаём таблицу
	createStruct()
	//заполняем таблицу тестовыми данными
	d := time.Now()
	fillTable()
	fmt.Printf("Таблица заполнена (%d записей) за %d мс. \n", totalRecs, time.Since(d).Milliseconds())
}

//создаём таблицу
func createStruct() {
	var err error
	if _, err = db.Exec("DROP TABLE IF EXISTS testTbl"); err != nil {
		log.Fatal(err)
	}
	q1 := `CREATE TABLE IF NOT EXISTS testTbl (
		 rid INTEGER NOT NULL PRIMARY KEY,
		 code INTEGER NOT NULL,
	    name TEXT NOT NULL,
	    mark INTEGER NOT NULL )`
	if _, err = db.Exec(q1); err != nil {
		log.Fatal(err)
	}
	//добавим индексы
	q2 := "CREATE UNIQUE INDEX IF NOT EXISTS idx_rid ON testTbl (rid)"
	if _, err = db.Exec(q2); err != nil {
		log.Fatal(err)
	}
	q3 := "CREATE INDEX IF NOT EXISTS idx_code ON testTbl (code)"
	if _, err = db.Exec(q3); err != nil {
		log.Fatal(err)
	}
	q4 := "CREATE UNIQUE INDEX IF NOT EXISTS idx_code_name ON  testTbl (code, name)"
	if _, err := db.Exec(q4); err != nil {
		log.Fatal(err)
	}
}

//заполняем таблицу тестовыми данными
func fillTable() {
	gen = rand.New(rand.NewSource( 0 /*time.Now().UnixNano()*/ ))
	//начинаем транзакцию
	tx, err := db.Begin()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Rollback() //страховка
	//цикл заполнения
	multi := 20
	rid := 0
	for rid < totalRecs {
		q := "INSERT INTO testTbl (rid, code, name, mark) VALUES "
		for i := 0; i < multi; i++ {
			if i > 0 {
				q = q + ", "
			}
			q = q + "(" + strconv.Itoa(rid) + "," + strconv.Itoa(int(100+gen.Int31n(900))) +
				",'" + buildRandomName() + "',0)"
			rid++
		}
		if _, err := tx.Exec(q); err != nil {
			log.Panic(err)
		}
	}
	//фиксация транзакции в БД
	if err = tx.Commit(); err != nil {
		log.Panic(err)
	}
}

//сгенерировать случайную строку
func buildRandomName() string {
	len := 15 + gen.Int31n(15)
	buf := make([]byte, len, len)
	for i := 0; i < int(len); i++ {
		buf[i] = byte(0x61 + gen.Int31n(26))
	}
	res := string(buf)
	return res
}

//завершаем работу
func done() {
	db.Close()
	fmt.Println("Замеры окончены.")
}

//сгенерировать случайный набор rid-ов
func fillRids(rids []int32) {
	//могут быть повторы, если надо без повторов - использовать map
	for i := 0; i < len(rids); i++ {
		rids[i] = gen.Int31n(totalRecs)
	}
	sort.Slice(rids, func(i, j int) bool { return rids[i] < rids[j] })
}

//замеряем среднее время работы функции обновления
func bench(fup fupdate, portion int) {
	rids := make([]int32, updateRecs)
	var name string
	var dsum int64 = 0
	for i := 0; i < testCount; i++ {
		//список строк надо менять при каждом вызове, иначе сервер БД кэширует страницы
		fillRids(rids)
		t := time.Now()
		name = fup(rids, portion)
		dsum += time.Since(t).Nanoseconds()
	}
	//усредняем
	avg := float64(dsum) / testCount / float64(time.Millisecond)
	if dur1 == 0 {
		dur1 = avg //первый замер
	}
	// коэффициент сравнения с первым методом
	k := float64(dur1) / avg

	fmt.Printf("%6.1f мс  x%-6.2f  %s \n", avg, k, name)
}

func updateStupid(rids []int32, portion int) string {
	res := "По одной записи авто-транзакцией, c параметром, без Prepare"
	q := "UPDATE testTbl SET mark=mark+1 WHERE rid=$1"
	for i := 0; i < len(rids); i++ {
		if _, err := db.Exec(q, rids[i]); err != nil {
			log.Panic(err)
		}
	}
	return res
}

func updatePrepared(rids []int32, portion int) string {
	res := "По одной записи авто-транзакцией, c Prepared параметром"
	q := "UPDATE testTbl SET mark=mark+1 WHERE rid=$1"
	stmt, err := db.Prepare(q)
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()
	for i := 0; i < len(rids); i++ {
		if _, err := stmt.Exec(rids[i]); err != nil {
			log.Panic(err)
		}
	}
	return res
}

func updateClassic1(rids []int32, portion int) string {
	res := "По одной записи авто-транзакцией, без параметров"
	for i := 0; i < len(rids); i++ {
		q := "UPDATE testTbl SET mark=mark+1 WHERE rid=" + strconv.Itoa(int(rids[i]))
		if _, err := db.Exec(q); err != nil {
			log.Panic(err)
		}
	}
	return res
}

func updateTransactPrep(rids []int32, portion int) string {
	res := "Одной транзакцией по одной записи, c Prepare"
	tx, err := db.Begin()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Rollback() //страховка
	//подготавливаем запрос
	q := "UPDATE testTbl SET mark=mark+1 WHERE rid=$1"
	stmt, err := tx.Prepare(q)
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()
	for i := 0; i < len(rids); i++ {
		if _, err := stmt.Exec(rids[i]); err != nil {
			log.Panic(err)
		}
	}
	//фиксация транзакции в БД
	if err = tx.Commit(); err != nil {
		log.Panic(err)
	}
	return res
}

func updateTransactPrep4(rids []int32, portion int) string {
	res := "Одной транзакцией, c Prepare, по 4 записи"
	//начинаем транзакцию
	tx, err := db.Begin()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Rollback() //страховка
	//подготавливаем запрос
	q := "UPDATE testTbl SET mark=mark+1 WHERE rid IN ($1, $2, $3, $4)"
	stmt, err := tx.Prepare(q)
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()
	for i := 0; i+4 <= len(rids); i += 4 {
		if _, err := stmt.Exec(rids[i+0], rids[i+1], rids[i+2], rids[i+3]); err != nil {
			log.Panic(err)
		}
	}
	//фиксация транзакции в БД
	if err = tx.Commit(); err != nil {
		log.Panic(err)
	}
	return res
}

func updateTransactPrep8(rids []int32, portion int) string {
	res := "Одной транзакцией, c Prepare, по 8 записей"
	//начинаем транзакцию
	tx, err := db.Begin()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Rollback() //страховка
	//подготавливаем запрос
	q := "UPDATE testTbl SET mark=mark+1 WHERE rid IN ($1, $2, $3, $4, $5, $6, $7, $8)"
	stmt, err := tx.Prepare(q)
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()
	for i := 0; i+8 <= len(rids); i += 8 {
		if _, err := stmt.Exec(
			rids[i+0], rids[i+1], rids[i+2], rids[i+3],
			rids[i+4], rids[i+5], rids[i+6], rids[i+7]); err != nil {
			log.Panic(err)
		}
	}
	//фиксация транзакции в БД
	if err = tx.Commit(); err != nil {
		log.Panic(err)
	}
	return res
}

func updateTransactTextN(rids []int32, portion int) string {
	res := fmt.Sprintf("Одной транзакцией, по %d записей текстом", portion)
	//начинаем транзакцию
	tx, err := db.Begin()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Rollback() //страховка
	i := 0
	for i < len(rids) {
		//подготавливаем запрос
		q := "UPDATE testTbl SET mark=mark+1 WHERE rid IN (" + strconv.Itoa(int(rids[i]))
		i++
		for j := 1; (j < portion) && (i < len(rids)); j++ {
			q = q + "," + strconv.Itoa(int(rids[i]))
			i++
		}
		q = q + ")"
		if _, err := tx.Exec(q); err != nil {
			log.Panic(err)
		}
	}
	//фиксация транзакции в БД
	if err = tx.Commit(); err != nil {
		log.Panic(err)
	}
	return res
}

func updateTextN(rids []int32, portion int) string {
	res := fmt.Sprintf("Авто-транзакции, по %d записей текстом", portion)
	i := 0
	for i < len(rids) {
		//подготавливаем запрос
		q := "UPDATE testTbl SET mark=mark+1 WHERE rid IN (" + strconv.Itoa(int(rids[i]))
		i++
		for j := 1; (j < portion) && (i < len(rids)); j++ {
			q = q + "," + strconv.Itoa(int(rids[i]))
			i++
		}
		q = q + ")"
		if _, err := db.Exec(q); err != nil {
			log.Panic(err)
		}
	}
	return res
}

func updateTempN(rids []int32, portion int) string {
	res := fmt.Sprintf("Через временную таблицу, insert по %d записей", portion)
	//начинаем транзакцию
	tx, err := db.Begin()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Rollback() //страховка
	//создаём временную таблицу
	qc := "CREATE TEMP TABLE tmpx (rid INTEGER) ON COMMIT DROP"
	if _, err := tx.Exec(qc); err != nil {
		log.Panic(err)
	}
	//загоняем идентификаторы во временную таблицу
	i := 0
	for i < len(rids) {
		//подготавливаем запрос
		q := "INSERT INTO tmpx (rid) VALUES (" + strconv.Itoa(int(rids[i])) + ")"
		i++
		for j := 1; (j < portion) && (i < len(rids)); j++ {
			q = q + ", (" + strconv.Itoa(int(rids[i])) + ")"
			i++
		}
		if _, err := tx.Exec(q); err != nil {
			log.Panic(err)
		}
	}
	//обновляем основную таблицу по временной таблице (431 ms)
	qt := "UPDATE testTbl SET mark=mark+1 FROM tmpx WHERE testTbl.rid=tmpx.rid"
	if _, err := tx.Exec(qt); err != nil {
		log.Panic(err)
	}
	//фиксация транзакции в БД
	if err = tx.Commit(); err != nil {
		log.Panic(err)
	}
	return res
}
