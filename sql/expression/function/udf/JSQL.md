# JSQL

This is part of the larger initiative to stop custom coding for all business rules and calling them micro-services. If people want, they can call these a **pico-service** design pattern.

![quis=persona; quid=factum; cur=causa; ubi=locus; quando=tempus; quemadmodum = modus; quib/adminiculis=facultas](https://upload.wikimedia.org/wikipedia/commons/4/41/Victorinus.gif)

## Why?

Because no developer should ever code for trivial business logic. It always starts with "just this much" and then before you notice it, whole business logic is embedded into your server source code which people "refactor" from monolith and start taking about "micro-service-architecture".  Hence the goals are:

1. Removal of data processing business logic from "hard" source code 
2. Put them into dynamic configuration 
3. Domain Specific ( SQL ) like configuration 
4. All Aggregator service & all micro services should be written as configuration - known as pico-service : p-Service. "P" quite literally can be stand for "pico" or "profitable" in terms fo ROI.

`GO` was used to create this infrastructure, because `GO` is the language for infrastructure. Reasonably low level, and reasonably fast. `RUST` could have been used - but lack of suitable first class libraries ( `RUST` does not have a **SQL Engine** Implementation ).

## When?

Where delivery speed matters. 

1. Lot of aggregation is happening inside source - lot of calls to lot of other micro-services. 
2. It is hard to change business logic inside the source code 
   1. Adding feature takes time
   2. Fixing bugs takes time 
3. Business people to maintain the business logic 
4. Business logic is completely out of the code 



## What / How?



### Understanding The Problem 

Imagine all external services and data sources as tables/views. Once we do it, then almost every business logic ( 80% of them ) would be about imagining  queries to be performed on them. That is declarative thinking.

Now, these queries imagine data sources to be APIs or in some mundane circumstances - DB.

Moreover, analysing this queries give automatic rise to parallelism - that is it eventually becomes a workflow graph, where individual nodes can be processed parallel.

Hence the problem is divided into 3 parts:

1. Call Data Sources
2. Process data from the sources by removing part not required and mapping parts which will be required ( Filter/Mapper )
3. Aggregation & Mapping back data again  

There is only one ubiquitous language for data processing : `SQL`.  This has been tried many times, `PIG` for `Hadoop`. Specifically yahoo did `YQL`. 

But that is one part of the problem. There is a major part of programmability - that is Turing Completeness. Neither `SQL` or `GraphQL` is Turing Complete - and hence is unusable for this purpose.

### Part of the Solution 

There is a Turing Complete language that can hold on it's own because of the popularity - it is `JavaScript`. Anyone and everyone can code in it. Hence one of the solution is clearly JavaScript enabled SQL and an workflow engine - where custom extension be written as JavaScript expression.

This is what we call `JSQL` - **JavaScript Enabled SQL**. 

### What Is Inside JSQL

This is non trivial. One has to have the following :

1. An Embeddable in Memory SQL Engine ( people had it - needed customisation )
2. Custom SQL Query Parser - to enable macros
3. Registration of Scripts as User Defined Functions - UDFS

The minimal macro idea is as follows:

```go
// +----------+-------------------+-------------------------------+---------------------+
// | name     | email             | phone_numbers                 | created_at          |
// +----------+-------------------+-------------------------------+---------------------+
// | John Doe | john@doe.com      | ["555-555-555"]               | 2018-04-18 09:41:13 |
// | John Doe | johnalt@doe.com   | []                            | 2018-04-18 09:41:13 |
// | Jane Doe | jane@doe.com      | []                            | 2018-04-18 09:41:13 |
// | Evil Bob | evilbob@gmail.com | ["555-666-555","666-666-666"] | 2018-04-18 09:41:13 |
// +----------+-------------------+-------------------------------+---------------------+
// ```
engine := sqle.NewDefault()
engine.AddDatabase(createTestDatabase())
engine.AddDatabase(sql.NewInformationSchemaDatabase(engine.Catalog))
// now query
ctx := sql.NewEmptyContext()
// this is where the macro comes in
query := "SELECT  <? @{mytable.phone_numbers}.length ?> FROM mytable"
_, it, e := engine.SQuery(ctx, query)	
```

This produce, as expected a list `[1,0,0,2]` .

What is really happening under the hood? The macro is being parsed ( notice the `<? .. inside . ?>` ) into custom scriptable UDF, which is automatically being registered inside the SQL engine. 

Naturally all these are open APIs ( extensions created) :

```go
// run macro processor 
processedQuery, customFunctions := udf.MacroProcessor(query)
// Register them...
_ = engine.RegisterUDF(customFunctions[i])
```

One can naturally create custom UDF by hand too:

```go
someUDF := ScriptUDF{Id: "my_name", Lang: "js", Body: expr}
_ = engine.RegisterUDF(someUDF)
```

#### Macro Specification 

1. Macros are defined within `<? ` And `?>`. Anything within that is a macro. Given a macro is compiled back using UDF which takes custom functions - it is necessary to delimit the parameters. 
2. Parameters are automatically defined because of the `@{param}` syntax. 
3. Given macro are JavaScript - all javascript syntax is valid inside it. 

Hence:

```sql
SELECT  <? @{mytable.phone_numbers}.length ?> FROM mytable
```

Gets converted into de facto:

```javascript
function _auto_1_udf_( phone_numbers ){
  return phone_numbers.length;
} 
```

Given the MySQL implementation automatically using `JSON` type, the UDF response is JSON type. Hence using macro enabled language - one can easily query nested `JSON` as well as array. 

There are aggregators too.  This is defined, naturally by :

```sql
-- this is how you use SET accumulator 
SELECT  <?SET@ @{mytable.phone_numbers}.length ?> FROM mytable
-- this is how you use LIST accumulator 
SELECT  <?LST@ @{mytable.phone_numbers}.length ?> FROM mytable
```

And, then there is a generic **FOLD**. Here is some code that calculates factorial on the number of rows of the table! 

```sql
SELECT  <?AGG@ [0,1] /* initial expression */ # $_[0] += 1 ; $_[1] *= $_[0] ; $_ ; /* expression body */ ?> FROM mytable
```

This is equivalent to the formal code of :

```scala
initial_expression = [ 0 , 1 ]
for ( table_rows ) {
  previous_iter_num = initial_expression[0]
  current_iter_num = previous_iter_num + 1 
  previous_factorial = initial_expression[1]
  current_factorial = previous_factorial * current_iter_num
  initial_expression[0] = current_iter_num
  initial_expression[1] = current_factorial 
  initial_expression // last statement 
}
initial_expression // returns 
```

Naturally it can be extended to a proper JSON too :

```sql
SELECT  <?AGG@ i={'i':0,'f':1}; # $_.i += 1 ; $_.f *= $_.i ; $_ ; ?> FROM mytable
```





## Where ?

This is suitable for replacing aggregation business logic, rather than innovation logic.  

## Who?

Once the system is up - UI can be created so that business people can use it directly. Infra team can create more of these services in standard kubernetes clusters to scale up or down when need arise.



## References

1. Interestingly, there are very few useful books on what exactly are micro-services - and I am on the thanks section of some of them : https://www.amazon.in/Scala-Microservices-Jatin-Puri/dp/1786469340 

2. Yahoo Query Language : https://en.wikipedia.org/wiki/Yahoo!_Query_Language

3. LinkedIn ParSeq is a sample workflow system : https://github.com/linkedin/parseq 

4. Reference implementation is here : https://github.com/nmondal/go-mysql-server/commits/master 

5. Aggregators : https://en.wikipedia.org/wiki/Aggregate_function 

6. FOLD : https://en.wikipedia.org/wiki/Fold_(higher-order_function) 

7. Go De Facto JavaScript Engine : https://github.com/robertkrimen/otto 

8. Apache PIG UDF : https://pig.apache.org/docs/r0.17.0/udf.html 

   

   

