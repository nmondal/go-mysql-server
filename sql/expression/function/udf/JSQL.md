# JSQL

This is part of the larger initiative to stop custom coding for all business rules. 

![quis=persona; quid=factum; cur=causa; ubi=locus; quando=tempus; quemadmodum = modus; quib/adminiculis=facultas](https://upload.wikimedia.org/wikipedia/commons/4/41/Victorinus.gif)

## Why?

**Because  developers should never code business logic inside application source code.** 

It always starts with "just this much" and then before anyone notices it, whole business logic is embedded into source code which people "refactor" from monolith and get into "micro-service-architecture".  

This is how one can do micro-services in the right way (by ) composition over inheritance. 

**Goals** for the JSQL macro embedding are:

1. Removal of data processing business logic from "hard" source code to put into "soft" configuration code 
2. Put them into dynamic configuration 
3. Domain Specific ( SQL ) like configuration 
4. All Aggregator service & all micro services should be written as configuration
5. Increase in testability 

`GO` was used to create this infrastructure, because `GO` is the language for infrastructure. Reasonably low level, and reasonably fast. `RUST` could have been used - but lack of suitable first class libraries ( `RUST` does not have a **SQL Engine** Implementation ).

## When?

Where delivery speed matters. 

1. Lot of aggregation is happening inside source - lot of calls to lot of other micro-services. 
2. It is hard to change business logic inside the source code 
   1. Adding feature takes time
   2. Fixing bugs takes time 
3. Business people to maintain the business logic 
4. Business logic is completely out of the code - and can be maintained as configuration with suitable UI to edit.



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

But that is one part of the problem. There is a major part of programmability - that is Turing Completeness. Neither `SQL` or `GraphQL` is Turing Complete - and hence is unusable for generic aggregation purpose. 

Observe the tiny problem of GraphQL unable to evaluate arbitrary expression - that is solved by .

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
_, it, e := engine.SQuery(ctx, query, "js") // tell the embedding language too 	
```

This produce, as expected a list `[1,0,0,2]` .

What is really happening under the hood? The macro is being parsed ( notice the `<? .. inside . ?>` ) into custom scriptable UDF, which is automatically being registered inside the SQL engine. 

Naturally all these are open APIs ( extensions created) :

```go
// run macro processor : query string, id of the udf, language of the embdedding 
processedQuery, customFunctions := udf.MacroProcessor(query, 0, "js")
// Register them...
_ = engine.RegisterUDF(customFunctions[i])
```

One can  create custom UDF by hand too:

```go
someUDF := ScriptUDF{Id: udfName, Script: GetScriptInstance(langDialect, expr),
			initial: initialAggregatorValue, UdfType: udfType}
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

### Specific Injected Variables 

The following variables are injected automatically as of now:

```go
/* 
Function which handles scripting 
ctx SQL context 
row is a typically an array of values - input
partial is accumulator value as of now ( for aggregation )
*/
func (a *Scriptable) EvalScript(ctx *sql.Context, row sql.Row, partial interface{}) (interface{}, error) 
```

| Variable in GO Engine                   | Script Variable | Explanation       |
| --------------------------------------- | --------------- | ----------------- |
| `row`                                   | `$ROW`          | Row or Input      |
| `ctx`                                   | `$CONTEXT`      | SQL Context       |
| Function parameters ( If not Anonymous) | `$ARGS`         | -                 |
| Accumulation partial value              | `$_`            | Accumulator Value |



### Specific Aggregation in Accumulator 

There are aggregators too.  This is defined, naturally by :

```sql
-- this is how you use SET accumulator 
SELECT  <?S__@ @{mytable.phone_numbers}.length ?> FROM mytable
-- this is how you use LIST accumulator 
SELECT  <?L__@ @{mytable.phone_numbers}.length ?> FROM mytable
```

Notice the basic idea. It is specifically defined as `<?XXX@` syntax. 

### Flatten & Transpose 

In many cases the nested query will be used to generate a list of options. This is exactly where "flatten" and "transpose" comes in.

Flatten is pretty simple idea, you have a nested collection and you reduce the nested collection to a flattened collection:

```js
nf = [1,2,[3,4],5,6] ;
f = flatten(nf);
//f = [1,2,3,4,5,6]
```

This is achieved via setting the 2nd character of the aggregators to `'F'`. 

SQL transpose is achieved via PIVOT, which our mysql go server did not support, so we wrote one, and gave a hook to the macro processor to PIVOT. 

 This is achieved via setting the 3rd character of the aggregators to `'T'`. 

Thus the whole specification of aggregation over flatter and transpose is given by the matrix:

```go
/**
Let's define the protocol : Here
L__ -> List, no flatten, no transpose
L_T -> List, no flatten, Transpose
LFT -> List, flatten, Transpose

S__ -> Set, no flatten, no transpose
S_T -> Set, no flatten, Transpose
SFT -> Set, flatten, Transpose

--> AGG can not have flatten it is upto author
AGG -> Aggregate, no flatten, no transpose
AGT -> AGG, no flatten, Transpose
*/

```

### Generic Aggregation 

And, then there is a generic **FOLD**. Here is some code that calculates factorial on the number of rows of the table! 

```sql
SELECT  <?AGG@ [0,1] /* initial expression */ # $_[0] += 1 ; $_[1] *= $_[0] ; $_ ; /* expression body */ ?> FROM mytable
```

Notice that the the extra `#` in the syntax of `<?AGX@ <init-expression>#`.  Anything between the `@` and the first `#` will be taken as the initial expression for the fold higher order function. 

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



1. Yahoo Query Language : https://en.wikipedia.org/wiki/Yahoo!_Query_Language

2. LinkedIn ParSeq is a sample workflow system : https://github.com/linkedin/parseq 

3. Reference implementation is here : https://github.com/nmondal/go-mysql-server/commits/master 

4. Aggregators : https://en.wikipedia.org/wiki/Aggregate_function 

5. FOLD : https://en.wikipedia.org/wiki/Fold_(higher-order_function) 

6. SQL PIVOT : https://stackoverflow.com/questions/13372276/simple-way-to-transpose-columns-and-rows-in-sql 

7. FLATTEN : https://rosettacode.org/wiki/Flatten_a_list 

8. Go De Facto JavaScript Engine : https://github.com/robertkrimen/otto 

9. Apache PIG UDF : https://pig.apache.org/docs/r0.17.0/udf.html 

10. Is GraphQL The Future : https://artsy.github.io/blog/2018/05/08/is-graphql-the-future/ 

11. Implementations of Field Resolvers : https://www.apollographql.com/docs/tutorial/resolvers/ 

12. Interestingly, there are very few useful books on what exactly are micro-services - (I am on the thanks section of at least one of them) : https://www.amazon.in/Scala-Microservices-Jatin-Puri/dp/1786469340 

     

     

