package sqle

import (
	"fmt"
	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

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
	table.Insert(ctx, sql.NewRow("John Doe", "john@doe.com", []string{"555-555-555"}, time.Now()))
	table.Insert(ctx, sql.NewRow("John Doe", "johnalt@doe.com", []string{}, time.Now()))
	table.Insert(ctx, sql.NewRow("Jane Doe", "jane@doe.com", []string{}, time.Now()))
	table.Insert(ctx, sql.NewRow("Evil Bob", "evilbob@gmail.com", []string{"555-666-555", "666-666-666"}, time.Now()))
	return db
}

func runAutoUDFEnabledQuery(query string, engine *Engine, ctx *sql.Context) ([]sql.Row, error) {
	_, it, e := engine.SQuery(ctx, query)
	if e != nil {
		fmt.Println("Error !!! ")
		fmt.Println(e)
		return nil, e
	}
	rows, _ := sql.RowIterToRows(it)
	for i := 0; i < len(rows); i++ {
		fmt.Println(rows[i])
	}
	return rows, nil
}

func TestGeneric_Agg_Factorial(t *testing.T) {
	assertions := require.New(t)
	engine := NewDefault()
	engine.AddDatabase(createTestDatabase())
	engine.AddDatabase(sql.NewInformationSchemaDatabase(engine.Catalog))
	// now query
	ctx := sql.NewEmptyContext()
	query := "SELECT <?AGG@ [ 0, 1 ] # $_[0] += 1 ; $_[1] *= $_[0] ; $_ ; ?>  FROM mytable"
	rows, e := runAutoUDFEnabledQuery(query, engine, ctx)
	assertions.Equal(nil, e)
	assertions.Equal(1, len(rows))
	finalRow := rows[0][0].([]int64)
	assertions.Equal(int64(4), finalRow[0])
	assertions.Equal(int64(24), finalRow[1])
	// do it JSON style ?
	query = "SELECT <?AGG@ x = { 'i' : 0, 'f' : 1 } # $_.i += 1 ; $_.f *= $_.i ; $_ ; ?>  FROM mytable"
	rows, e = runAutoUDFEnabledQuery(query, engine, ctx)
	assertions.Equal(nil, e)
	assertions.Equal(1, len(rows))
	finalJSONRow := rows[0][0].(map[string]interface{})
	assertions.Equal(2, len(finalJSONRow))
	// TODO find out what is going wrong here? This should not be float...
	assertions.Equal(float64(4), finalJSONRow["i"])
	assertions.Equal(float64(24), finalJSONRow["f"])
}

func TestGeneric_UDFS(t *testing.T) {
	assertions := require.New(t)
	engine := NewDefault()
	engine.AddDatabase(createTestDatabase())
	engine.AddDatabase(sql.NewInformationSchemaDatabase(engine.Catalog))
	// now query
	ctx := sql.NewEmptyContext()
	query := "SELECT <? @{mytable.name}.length; ?> ,  <? @{mytable.phone_numbers}.length; ?>  FROM mytable"
	rows, e := runAutoUDFEnabledQuery(query, engine, ctx)
	assertions.Equal(nil, e)
	assertions.Equal(4, len(rows))
}

func TestGeneric_Agg_Collector(t *testing.T) {
	assertions := require.New(t)
	engine := NewDefault()
	engine.AddDatabase(createTestDatabase())
	engine.AddDatabase(sql.NewInformationSchemaDatabase(engine.Catalog))
	// now query
	ctx := sql.NewEmptyContext()
	query := "SELECT <?LST@ x = { 'n' : @{mytable.name} , 'm' : @{mytable.email} }; ?>  FROM mytable"
	rows, e := runAutoUDFEnabledQuery(query, engine, ctx)
	assertions.Equal(nil, e)
	assertions.Equal(1, len(rows))
	finalRow := rows[0][0].([]interface{})
	assertions.Equal(4, len(finalRow))

	// do it SET ?
	query = "SELECT <?SET@ @{mytable.name} ?>  FROM mytable"
	rows, e = runAutoUDFEnabledQuery(query, engine, ctx)
	assertions.Equal(nil, e)
	assertions.Equal(1, len(rows))
	finalRow = rows[0][0].([]interface{})
	assertions.Equal(3, len(finalRow))
}
