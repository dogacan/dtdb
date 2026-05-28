#pragma once
#include "rust/cxx.h"
#include "dtdb_bindings/src/lib.rs.h"
#include <functional>
#include <type_traits>
#include <string>
#include <vector>
#include <utility>
#include <cstddef>

namespace dtdb {

class SqlQuery {
public:
    template <size_t N>
    explicit SqlQuery(const char (&text)[N]) : text_(text) {}
    
    template <size_t N>
    SqlQuery& bind(const char (&name)[N], const std::string& value) {
        params_.push_back({name, value, ParamKind::Text});
        return *this;
    }

    template <size_t N, typename T>
    typename std::enable_if_t<std::is_arithmetic_v<T>, SqlQuery&>
    bind(const char (&name)[N], T value) {
        if constexpr (std::is_same_v<T, bool>) {
            params_.push_back({name, value ? "1" : "0", ParamKind::Bool});
        } else if constexpr (std::is_floating_point_v<T>) {
            params_.push_back({name, std::to_string(value), ParamKind::Float});
        } else {
            params_.push_back({name, std::to_string(value), ParamKind::Int});
        }
        return *this;
    }

    template <size_t N>
    SqlQuery& bind(const char (&name)[N], const std::vector<uint8_t>& value) {
        // Lowercase hex with no delimiters; the Rust side decodes it directly.
        std::string hex_str;
        hex_str.reserve(value.size() * 2);
        for (uint8_t byte : value) {
            static const char hex[] = "0123456789abcdef";
            hex_str.push_back(hex[byte >> 4]);
            hex_str.push_back(hex[byte & 0xf]);
        }
        params_.push_back({name, hex_str, ParamKind::Bytes});
        return *this;
    }

    template <size_t N>
    SqlQuery& bind(const char (&name)[N], std::nullptr_t) {
        params_.push_back({name, std::string(), ParamKind::Null});
        return *this;
    }

    // Conversion helper to bridge type
    CxxSqlQuery to_bridge() const {
        rust::Vec<QueryParam> bridge_params;
        for (const auto& p : params_) {
            bridge_params.push_back(QueryParam{p.name, p.value, p.kind});
        }
        return CxxSqlQuery{text_, bridge_params};
    }
private:
    struct Param {
        std::string name;
        std::string value;
        ParamKind kind;
    };
    std::string text_;
    std::vector<Param> params_;
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

    // Exception-safe counterpart to run_in_transaction.
    //
    // The rollback handler must only run while `tx` is still valid (i.e. only
    // for exceptions thrown by the user callable). If `commit_tx` itself
    // throws after consuming `tx` via std::move, jumping back to a single
    // outer catch would re-move from a moved-from rust::Box — undefined
    // behavior. Scope the rollback try/catch tightly around the user code so
    // commit_tx exceptions propagate naturally without touching `tx` again.
    template <typename Func>
    auto run_in_transaction(const std::string& db_name, Func func)
        -> decltype(func(std::declval<const CxxTransaction&>()))
    {
        rust::Box<CxxTransaction> tx = inner_->start_transaction(db_name);
        if constexpr (std::is_void_v<decltype(func(*tx))>) {
            try {
                func(*tx);
            } catch (...) {
                inner_->rollback_tx(std::move(tx));
                throw;
            }
            inner_->commit_tx(std::move(tx));
        } else {
            auto run = [&]() {
                try {
                    return func(*tx);
                } catch (...) {
                    inner_->rollback_tx(std::move(tx));
                    throw;
                }
            };
            auto val = run();
            inner_->commit_tx(std::move(tx));
            return val;
        }
    }

private:
    rust::Box<CxxClient> inner_;
};

} // namespace dtdb
