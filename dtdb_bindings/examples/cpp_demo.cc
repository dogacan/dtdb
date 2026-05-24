#include "dtdb_bindings/include/dtdb.h"
#include <iostream>
#include <string>
#include <vector>
#include <cassert>
#include <filesystem>

namespace fs = std::filesystem;

int main() {
    std::cout << "[C++] Starting DuctTapeDB C++ FFI Demo..." << std::endl;

    std::string db_dir = "./cpp_demo_db";
    if (fs::exists(db_dir)) {
        std::cout << "[C++] Cleaning old database directory: " << db_dir << std::endl;
        fs::remove_all(db_dir);
    }

    try {
        // 1. Initialize the in-process client using the clean dtdb::Client wrapper
        std::cout << "[C++] Initializing in-process client at " << db_dir << "..." << std::endl;
        dtdb::Client client = dtdb::Client::InProcess(db_dir);

        // 2. Create a new database
        std::cout << "[C++] Creating database 'demo_db'..." << std::endl;
        client.create_db("demo_db");

        // 3. Create a table
        std::cout << "[C++] Creating table 'users'..." << std::endl;
        QueryResult create_res = client.execute_query(
            "demo_db",
            dtdb::SqlQuery("CREATE TABLE users (id INT PRIMARY KEY, name STRING, active INT);")
        );
        assert(create_res.success);
        std::cout << "[C++] Create table success: " << std::string(create_res.rows[0]) << std::endl;

        // 4. Executing inserts inside an exception-safe transaction block using parameterized queries
        std::cout << "[C++] Executing inserts in run_in_transaction using parameterized queries..." << std::endl;
        client.run_in_transaction("demo_db", [&](const CxxTransaction& tx) {
            QueryResult insert1 = client.execute_tx_query(
                tx,
                dtdb::SqlQuery("INSERT INTO users (id, name, active) VALUES (@id, @name, @active);")
                    .bind("id", 1)
                    .bind("name", "Alice")
                    .bind("active", true)
            );
            assert(insert1.success);

            QueryResult insert2 = client.execute_tx_query(
                tx,
                dtdb::SqlQuery("INSERT INTO users (id, name, active) VALUES (@id, @name, @active);")
                    .bind("id", 2)
                    .bind("name", "Bob")
                    .bind("active", false)
            );
            assert(insert2.success);

            QueryResult insert3 = client.execute_tx_query(
                tx,
                dtdb::SqlQuery("INSERT INTO users (id, name, active) VALUES (@id, @name, @active);")
                    .bind("id", 3)
                    .bind("name", "Charlie")
                    .bind("active", true)
            );
            assert(insert3.success);
        });
        std::cout << "[C++] Transaction committed successfully!" << std::endl;

        // 5. Query the inserted data with parameterized filtering
        std::cout << "[C++] Querying all active users (active = 1) using parameterized query..." << std::endl;
        QueryResult select_res = client.execute_query(
            "demo_db",
            dtdb::SqlQuery("SELECT id, name FROM users WHERE active = @active ORDER BY id ASC;")
                .bind("active", true)
        );
        assert(select_res.success);

        // Decode 1D flattened row values
        size_t cols = select_res.headers.size();
        size_t rows = cols > 0 ? select_res.rows.size() / cols : 0;
        
        std::cout << "\n[C++] --- Query Results ---" << std::endl;
        for (const auto& h : select_res.headers) {
            std::cout << std::string(h) << "\t";
        }
        std::cout << "\n---------------------------" << std::endl;

        for (size_t r = 0; r < rows; ++r) {
            for (size_t c = 0; c < cols; ++c) {
                std::cout << std::string(select_res.rows[r * cols + c]) << "\t";
            }
            std::cout << std::endl;
        }
        std::cout << "---------------------------\n" << std::endl;

        // 6. Test Error Handling (query non-existent table)
        std::cout << "[C++] Testing FFI error handling (should throw exception)..." << std::endl;
        try {
            client.execute_query("demo_db", dtdb::SqlQuery("SELECT * FROM non_existent;"));
            std::cerr << "[C++] Error: Expected exception, but query succeeded!" << std::endl;
            return 1;
        } catch (const rust::Error& e) {
            std::cout << "[C++] Success: Caught expected database exception: " << e.what() << std::endl;
        }

        std::cout << "[C++] Cleaning up database directory..." << std::endl;
        fs::remove_all(db_dir);
        std::cout << "[C++] Demo completed successfully!" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "[C++] Unhandled exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
