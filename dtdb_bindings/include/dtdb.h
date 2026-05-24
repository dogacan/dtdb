#pragma once
#include "rust/cxx.h"
#include "dtdb_bindings/src/lib.rs.h"
#include <functional>
#include <type_traits>
#include <string>

namespace dtdb {

class Client {
public:
    explicit Client(rust::Box<CxxClient> inner) : inner_(std::move(inner)) {}

    // Factory methods
    static Client InProcess(const std::string& data_dir) {
        return Client(new_in_process_client(data_dir));
    }

    static Client Remote(const std::string& server_address) {
        return Client(new_remote_client(server_address));
    }

    void create_db(const std::string& db_name) {
        inner_->create_db(db_name);
    }

    void drop_db(const std::string& db_name) {
        inner_->drop_db(db_name);
    }

    QueryResult execute_query(const std::string& db_name, const std::string& sql) {
        return inner_->execute_query(db_name, sql);
    }

    QueryResult execute_tx_query(const CxxTransaction& tx, const std::string& sql) {
        return inner_->execute_tx_query(tx, sql);
    }

    // Exception-safe counterpart to run_in_transaction
    template <typename Func>
    auto run_in_transaction(const std::string& db_name, Func func) 
        -> decltype(func(std::declval<const CxxTransaction&>())) 
    {
        rust::Box<CxxTransaction> tx = inner_->start_transaction(db_name);
        try {
            if constexpr (std::is_void_v<decltype(func(*tx))>) {
                func(*tx);
                inner_->commit_tx(std::move(tx));
            } else {
                auto val = func(*tx);
                inner_->commit_tx(std::move(tx));
                return val;
            }
        } catch (...) {
            inner_->rollback_tx(std::move(tx));
            throw; // Rethrow FFI or user exceptions
        }
    }

private:
    rust::Box<CxxClient> inner_;
};

} // namespace dtdb
