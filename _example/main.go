package main

import (
	"fmt"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/auth"
	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/server"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression/function/udf"
	"time"
)

// Example of how to implement a MySQL server based on a Engine:
//
// ```
// > mysql --host=127.0.0.1 --port=5123 -u user -ppass db -e "SELECT * FROM mytable"
// +----------+-------------------+-------------------------------+---------------------+
// | name     | email             | phone_numbers                 | created_at          |
// +----------+-------------------+-------------------------------+---------------------+
// | John Doe | john@doe.com      | ["555-555-555"]               | 2018-04-18 09:41:13 |
// | John Doe | johnalt@doe.com   | []                            | 2018-04-18 09:41:13 |
// | Jane Doe | jane@doe.com      | []                            | 2018-04-18 09:41:13 |
// | Evil Bob | evilbob@gmail.com | ["555-666-555","666-666-666"] | 2018-04-18 09:41:13 |
// +----------+-------------------+-------------------------------+---------------------+
// ```
func main() {
	engine := sqle.NewDefault()
	engine.AddDatabase(createTestDatabase())
	engine.AddDatabase(sql.NewInformationSchemaDatabase(engine.Catalog))
	// now query
	ctx := sql.NewEmptyContext()
	query := "SELECT <? @{mytable.phone_numbers}.length ?> FROM mytable WHERE <? @{mytable.phone_numbers}.length ?> = 0 "
	//query := "SELECT ltrim(mytable.name)  FROM mytable WHERE length(mytable.name)!= 0 "
	runAutoUDFEnabledQuery(query, engine, ctx)
}

func runAutoUDFEnabledQuery(query string, engine *sqle.Engine, ctx *sql.Context) {
	_, it, e := engine.SQuery(ctx, query, "js")
	if e != nil {
		fmt.Println("Error !!! ")
		fmt.Println(e)
		return
	}
	rows, _ := sql.RowIterToRows(it)
	for i := 0; i < len(rows); i++ {
		fmt.Println(rows[i])
	}
}

func testAutoUDF() {
	q := "SELECT  <? @{mytable.phone_numbers}.length ?> FROM mytable"
	pq, udfs := udf.MacroProcessor(q, 0, "js")
	fmt.Println(pq)
	fmt.Println(udfs)
}

func startServer(engine *sqle.Engine) {
	config := server.Config{
		Protocol: "tcp",
		Address:  "localhost:3306",
		Auth:     auth.NewNativeSingle("root", "", auth.AllPermissions),
	}

	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		panic(err)
	}
	_ = s.Start()
}

func createTestDatabase() *memory.Database {
	const (
		dbName    = "mydb"
		tableName = "mytable"
	)

	db := memory.NewDatabase(dbName)
	table := memory.NewTable(tableName, sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "email", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
		{Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: tableName},
	})

	db.AddTable(tableName, table)
	ctx := sql.NewEmptyContext()
	table.Insert(ctx, sql.NewRow("John Doe", "john@doe.com", []string{"555-555-555", "040-2406-7468"}, time.Now()))
	table.Insert(ctx, sql.NewRow("John Doe", "johnalt@doe.com", []string{}, time.Now()))
	table.Insert(ctx, sql.NewRow("Jane Doe", "jane@doe.com", []string{}, time.Now()))
	table.Insert(ctx, sql.NewRow("Evil Bob", "evilbob@gmail.com", []string{"555-555-555", "666-666-666",
		"444-555", "123-456"}, time.Now()))
	return db
}
