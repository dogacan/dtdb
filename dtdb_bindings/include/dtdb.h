#pragma once
#include "rust/cxx.h"
#include "dtdb_bindings/src/lib.rs.h"
#include <functional>
#include <type_traits>
#include <string>
#include <vector>
#include <utility>

namespace dtdb {

class SqlQuery {
public:
    template <size_t N>
    explicit SqlQuery(const char (&text)[N]) : text_(text) {}
    
    template <size_t N>
    SqlQuery& bind(const char (&name)[N], const std::string& value) {
        params_.push_back({name, value});
        return *this;
    }
    
    // Conversion helper to bridge type
    CxxSqlQuery to_bridge() const {
        rust::Vec<QueryParam> bridge_params;
        for (const auto& p : params_) {
            bridge_params.push_back(QueryParam{p.first, p.second});
        }
        return CxxSqlQuery{text_, bridge_params};
    }
private:
    std::string text_;
    std::vector<std::pair<std::string, std::string>> params_;
};

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

    QueryResult execute_query(const std::string& db_name, const SqlQuery& query) {
        return inner_->execute_query(db_name, query.to_bridge());
    }

    QueryResult execute_tx_query(const CxxTransaction& tx, const SqlQuery& query) {
        return inner_->execute_tx_query(tx, query.to_bridge());
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
