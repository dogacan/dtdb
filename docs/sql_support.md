# SQL Support in DuctTapeDB

DuctTapeDB (`dtdb`) supports a structured subset of standard SQL. Below is the documentation of all supported statements, data types, expressions, operators, and aggregate functions.

---

## 1. Supported Statements

### Data Definition Language (DDL)

#### `CREATE TABLE`
Creates a new table catalog entry and initializes the underlying LSM storage engine.
*   **Syntax**:
    ```sql
    CREATE TABLE <table_name> (
        <column_name> <data_type> [PRIMARY KEY | NOT NULL],
        ...
    );
    ```
*   **Supported Data Types**:
    *   `INT` / `INTEGER` / `BIGINT`: Mapped to 64-bit signed integers (`i64`).
    *   `FLOAT` / `DOUBLE` / `REAL`: Mapped to double-precision floats (`f64`).
    *   `TEXT` / `VARCHAR(N)` / `CHAR(N)` / `STRING`: Mapped to UTF-8 strings (`String`).
    *   `BYTEA` / `BLOB`: Mapped to byte arrays (`Vec<u8>`).
    *   `NULL`: Represents the absence of a value.
*   **Constraints & Nullability**:
    *   Each table must define exactly one column as the `PRIMARY KEY` (which serves as the LSM key and is implicitly `NOT NULL`).
    *   Columns are nullable by default. To disallow `NULL` values in a column, add the `NOT NULL` constraint.
    *   Inserting or updating a row to set a `NOT NULL` or `PRIMARY KEY` column to `NULL` results in a schema mismatch error.
*   **Transaction Restriction**:
    *   `CREATE TABLE` is a DDL statement and is **not allowed** inside explicit multi-statement transactions. It must be run as a single auto-committed statement.
*   **Example**:
    ```sql
    CREATE TABLE Users (
        id INT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        score DOUBLE
    );
    ```

#### `DROP TABLE`
Removes the table catalog entry and deletes the corresponding storage directory on disk.
*   **Syntax**:
    ```sql
    DROP TABLE <table_name>;
    ```
*   **Transaction Restriction**:
    *   `DROP TABLE` is a DDL statement and is **not allowed** inside explicit multi-statement transactions. It must be run as a single auto-committed statement.
*   **Concurrency & Serialization**:
    *   `DROP TABLE` utilizes **catalog-level serialization** by acquiring an exclusive write lock on the database catalog.
    *   If active transactions are currently accessing (reading from or writing to) the target table, `DROP TABLE` will block and wait for those transactions to complete before proceeding.
    *   While `DROP TABLE` is waiting or executing, any new transactions attempting to access *any* table in the database will block until the `DROP TABLE` operation completes.
*   **Example**:
    ```sql
    DROP TABLE Users;
    ```

---

### Data Manipulation Language (DML) & Queries

#### `INSERT INTO`
Inserts new rows into a table.
*   **Syntax**:
    ```sql
    INSERT INTO <table_name> [(<column_name>, ...)] VALUES (<literal_value>, ...), ...;
    ```
*   **Notes**:
    *   If no columns are specified, values must align with the columns in the exact order declared in the `CREATE TABLE` schema.
    *   Only literal values are allowed (not nested expressions or variables).
*   **Example**:
    ```sql
    INSERT INTO Users (id, name, score) VALUES (1, 'Alice', 95.5), (2, 'Bob', 88.0);
    ```

#### `UPDATE`
Updates existing rows in a table.
*   **Syntax**:
    ```sql
    UPDATE <table_name> SET <column_name> = <expression>, ... [WHERE <predicate>];
    ```
*   **Notes**:
    - Updates modifying primary key columns are planned as a `DELETE` followed by an `INSERT` under the hood.
*   **Example**:
    ```sql
    UPDATE Users SET score = score + 5.0, name = 'Alice Updated' WHERE id = 1;
    ```

#### `DELETE`
Deletes rows from a table.
*   **Syntax**:
    ```sql
    DELETE FROM <table_name> [WHERE <predicate>];
    ```
*   **Example**:
    ```sql
    DELETE FROM Users WHERE score < 50.0;
    ```

#### `SELECT`
Queries rows from table relations.
*   **Syntax**:
    ```sql
    SELECT <projection>
    FROM <table_name>
    [JOIN | LEFT JOIN <other_table> ON <join_condition>]
    [WHERE <predicate>]
    [GROUP BY <group_by_columns>]
    [ORDER BY <sort_columns>]
    [LIMIT <number>] [OFFSET <number>];
    ```
*   **Clauses Detail**:
    *   **Projection**: Supports columns, expressions, aliases (`col AS alias`), aggregate functions, and wildcard (`*`).
    *   **JOIN**: Supports inner and left outer equality joins (e.g. `ON t1.id = t2.user_id` or `LEFT JOIN ... ON ...`). Non-equality joins or right/full outer joins are not supported. Unmatched left rows are padded with type-default values for the right-side columns.
    *   **WHERE**: Filters source tuples using comparison and logical operators.
    *   **GROUP BY**: Groups rows by one or more columns for aggregation. When grouping, non-aggregate expressions in the select list are restricted to grouping columns.
    *   **ORDER BY**: Orders results by one or more expressions in ascending (`ASC`, default) or descending (`DESC`) order. You can sort by columns that are not projected in the select list.
    *   **LIMIT**: Restricts the maximum number of rows returned. Optional `OFFSET` skips a specified number of rows before returning results.
*   **Examples**:
    ```sql
    -- Simple select with sorting and limit
    SELECT name, score FROM Users WHERE score > 90.0 ORDER BY score DESC LIMIT 10;
    
    -- Inner Join
    SELECT Users.name, Orders.amount 
    FROM Users JOIN Orders ON Users.id = Orders.user_id;
    
    -- Grouped Aggregation
    SELECT country, COUNT(*), MAX(score) 
    FROM Users 
    GROUP BY country;
    ```

#### `EXPLAIN`
Displays the query execution plans generated by the planner and optimizer.
*   **Syntax**:
    ```sql
    EXPLAIN <select_statement>;
    ```
*   **Output**:
    Returns a tabular text representing the **Logical Plan**, **Optimized Plan**, and **Physical Plan** (Volcano operators).
*   **Example**:
    ```sql
    EXPLAIN SELECT name FROM Users WHERE id = 10;
    ```

---

## 2. Supported Expressions & Literals

*   **Identifiers**: Direct column names (e.g., `name`) or qualified identifiers (e.g., `Users.name`).
*   **Numeric Literals**: Integers (`42`) and floating-point numbers (`3.14`).
*   **String Literals**:
    *   Single-quoted string literals: `'text'`
    *   Double-quoted string literals: `"text"` (mapped to string values for MySQL/SQLite compatibility).
*   **Boolean Literals**: `TRUE` and `FALSE` (internally mapped to `1` and `0` integers).
*   **NULL Literal**: The keyword `NULL` represents a missing or unknown value.
*   **Parentheses**: Supported for nesting expressions (e.g., `(a AND b) OR c`).

---

## 3. Operators

### Comparison Operators
Used to compare expressions:
*   `=`: Equal to
*   `>`: Greater than
*   `<`: Less than
*   `>=`: Greater than or equal to
*   `<=`: Less than or equal to
*   `<>` / `!=`: Not equal to

### Logical Operators
Used to combine boolean expressions:
*   `AND`: Logical conjunction
*   `OR`: Logical disjunction

### Pattern Matching
*   `LIKE`: Performs simple wildcard string matching using `%`.
    *   `%`: Matches zero or more characters.
    *   *Example*: `WHERE name LIKE 'A%'` (starts with 'A'), `WHERE name LIKE '%b%'` (contains 'b').
    *   *Note*: `NOT LIKE` is not supported.

### Arithmetic Operators
Used for mathematical computations in select projections or predicates:
*   `+`: Addition
*   `-`: Subtraction
*   `*`: Multiplication
*   `/`: Division (performs integer division if both operands are integers, float division otherwise)

---

## 4. Aggregate Functions

Supported inside the projection list of a `SELECT` statement:
*   `COUNT(*)` / `COUNT(col)`: Counts the number of non-null values.
*   `SUM(col)`: Computes the sum of numeric values.
*   `MIN(col)`: Returns the minimum value in the group.
*   `MAX(col)`: Returns the maximum value in the group.
*   `AVG(col)`: Computes the average of numeric column values.

---

## 5. Scalar & Conditional Functions

### Conditional Expressions

#### `CASE WHEN ... THEN ... ELSE END`
Executes conditional logic matching branches in order. Supported in select projections, sorting, filters, and update assignments.

*   **Searched CASE**:
    Evaluates conditional predicates sequentially:
    ```sql
    CASE WHEN <condition1> THEN <result1> [WHEN <condition2> THEN <result2> ...] [ELSE <else_result>] END
    ```
    *   *Example*: `SELECT CASE WHEN price >= 1000 THEN 'expensive' ELSE 'cheap' END FROM products;`
*   **Simple CASE**:
    Compares an operand expression with branch keys:
    ```sql
    CASE <operand> WHEN <val1> THEN <result1> [WHEN <val2> THEN <result2> ...] [ELSE <else_result>] END
    ```
    *   *Example*: `SELECT CASE id WHEN 1 THEN 'One' WHEN 2 THEN 'Two' ELSE 'Other' END FROM products;`
*   *Note on Default Fallback*: If no branch matches and no `ELSE` is provided, `NULL` is returned.

### Scalar Functions

#### `LENGTH(<str>)`
Returns the character length of the input string or value (with implicit coercion to string).
*   *Example*: `SELECT LENGTH(name) FROM products;`

#### `SUBSTR(<str>, <start> [, <length>])` / `SUBSTRING(...)`
Extracts a substring starting at the 1-based index `start`.
*   Supports negative `start` values (counting backwards from the end of the string).
*   If `length` is omitted, extracts to the end of the string.
*   *Examples*:
    *   `SUBSTR('Laptop', 1, 3)` -> `'Lap'`
    *   `SUBSTR('Laptop', 4)` -> `'top'`
    *   `SUBSTR('Laptop', -3, 2)` -> `'to'`

#### `COALESCE(<arg1>, <arg2>, ...)`
Returns the first non-null argument.
*   **Behavior**: Evaluation proceeds left-to-right, returning the first argument that is not explicitly `NULL`.
*   *Example*: `SELECT COALESCE(category, 'Uncategorized') FROM products;`

---

## 6. Transactions & DDL Restrictions

DuctTapeDB supports explicit multi-statement transactions using a stream-based API or the client `run_in_transaction` method.

### DDL Concurrency & Serialization
*   **Catalog Locking**: DDL statements (`CREATE TABLE` and `DROP TABLE`) are serialized database-wide. They acquire an exclusive write lock on the database catalog, preventing concurrent transactions from starting or accessing any tables until the DDL operation completes.
*   **Active Transactions**: If active transactions are already accessing a table that is being dropped, `DROP TABLE` will block and wait for them to finish before modifying the catalog or deleting files on disk.

### Transaction Boundaries
*   **Supported inside Transactions**:
    *   DML statements: `INSERT`, `UPDATE`, `DELETE`.
    *   Queries: `SELECT`, `EXPLAIN`.
*   **Disabled inside Transactions**:
    *   DDL statements: `CREATE TABLE` and `DROP TABLE` are not allowed inside explicit transactions. Attempting to execute them within a transaction yields a validation error immediately, leaving the transaction session active for rollback or commit of other statements.
    *   DDL statements must instead be executed as single, auto-committed statements outside explicit transactions.
