# C++ & Swift Bindings (`dtdb_bindings`)

DuctTapeDB provides a unified FFI binding crate `dtdb_bindings` using the [`cxx`](https://cxx.rs/) library. This library bridges Rust's async client API to a clean, synchronous C++ API. 

Because the underlying core database engine (`dtdb_storage`, `dtdb_relational`, `dtdb_sql`) is entirely synchronous, the bindings run high-performance in-process query executions directly without needing to manage async runtimes in C++ or Swift.

---

## 🏗️ FFI Architecture

The FFI wrapper runs a background thread-pool (Tokio runtime) internally in Rust. When C++ invokes a synchronous FFI method, the Rust wrapper executes the request on the Tokio runtime and blocks the calling C++ thread until the database operation completes.

Transactions are managed using thread-safe, non-blocking channels that route FFI executions directly into DuctTapeDB's native transaction manager (`run_in_transaction`), ensuring transaction scopes are fully closed, committed, or rolled back safely upon completion or drop.

---

## 💻 C++ API Usage

We provide an idiomatic C++ wrapper class (`dtdb::Client`) located in `dtdb_bindings/include/dtdb.h`. This wrapper abstracts the FFI types (like `rust::Box` and `rust::Str`), maps Rust strings to `std::string`, and implements a template-based `run_in_transaction` helper.

### C++ Example

Here is how you can use the bindings inside a C++ application:

```cpp
#include "dtdb_bindings/include/dtdb.h"
#include <iostream>
#include <cassert>

int main() {
    try {
        // 1. Initialize the client (In-Process mode)
        dtdb::Client client = dtdb::Client::InProcess("./cpp_db");

        // Or connect to a remote gRPC server (Remote mode):
        // dtdb::Client client = dtdb::Client::Remote("http://127.0.0.1:50051");

        // 2. Create a database
        client.create_db("mydb");

        // 3. Create a table
        client.execute_query("mydb", dtdb::SqlQuery("CREATE TABLE users (id INT PRIMARY KEY, name STRING);"));

        // 4. Run multiple statements atomically in a transaction block using parameterized queries
        client.run_in_transaction("mydb", [&](const CxxTransaction& tx) {
            client.execute_tx_query(tx, dtdb::SqlQuery("INSERT INTO users VALUES (@id, @name);").bind("id", "1").bind("name", "Alice"));
            client.execute_tx_query(tx, dtdb::SqlQuery("INSERT INTO users VALUES (@id, @name);").bind("id", "2").bind("name", "Bob"));
        });
        std::cout << "Transaction committed successfully!" << std::endl;

        // 5. Query and decode rows
        QueryResult res = client.execute_query("mydb", dtdb::SqlQuery("SELECT * FROM users ORDER BY id ASC;"));
        size_t cols = res.headers.size();
        size_t rows = cols > 0 ? res.rows.size() / cols : 0;

        for (size_t r = 0; r < rows; ++r) {
            std::cout << "User " << std::string(res.rows[r * cols + 0]) 
                      << ": " << std::string(res.rows[r * cols + 1]) << std::endl;
        }

    } catch (const rust::Error& e) {
        // Caught database engine or transaction conflict exception
        std::cerr << "Database conflict or error: " << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Standard error: " << e.what() << std::endl;
    }
    return 0;
}
```

---

## 🔨 Building and Linking

### 1. Compile the Rust Static Library
Run cargo to build the workspace. This outputs the C++ static library `libdtdb_bindings.a` and generates FFI header files:

```bash
cargo build
```

This generates:
*   The static library: `target/debug/libdtdb_bindings.a`
*   The FFI headers directory: `target/cxxbridge/`

### 2. Compile the C++ Target
To compile your C++ application, include `-I.` and `-I./target/cxxbridge` to let your compiler resolve `#include "rust/cxx.h"` and `#include "dtdb_bindings/src/lib.rs.h"`. 

On macOS, you must link system libraries required by Rust's networking and concurrency runtime:

```bash
clang++ -std=c++17 -I. -I./target/cxxbridge \
  your_app.cc ./target/debug/libdtdb_bindings.a -o your_app \
  -lpthread -ldl -lresolv \
  -framework Security -framework CoreFoundation -framework SystemConfiguration
```

---

## 🍎 Swift Integration (Swift 5.9+)

Swift 5.9 features native C++ interoperability. You can import the FFI headers directly without writing a manual Objective-C bridging header.

### 1. Define a Module Map (`module.modulemap`)
Create a module map to expose the generated header files as a Swift module:

```modulemap
module DuctTapeDB {
    header "dtdb_bindings/include/dtdb.h"
    requires cplusplus
}
```

### 2. Import and Run in Swift
Link your Swift project to `libdtdb_bindings.a` and compile with `-cxx-interoperability-mode=default`. You can then call `dtdb::Client` methods natively:

```swift
import DuctTapeDB

func runDb() {
    do {
        // Instantiate the C++ class natively in Swift
        let client = dtdb.Client.InProcess("./swift_db")
        
        try client.create_db("swift_demo")
        try client.execute_query("swift_demo", dtdb.SqlQuery("CREATE TABLE items (id INT PRIMARY KEY);"))
        
        // Execute transaction block using Swift closure
        try client.run_in_transaction("swift_demo") { tx in
            _ = try client.execute_tx_query(tx, dtdb.SqlQuery("INSERT INTO items VALUES (@id);").bind("id", "1"))
        }
        
    } catch {
        print("Database execution failed: \(error)")
    }
}
```
