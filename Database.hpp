// Database.hpp
#ifndef DATABASE_HPP
#define DATABASE_HPP

#include "Table.hpp"
#include <unordered_map>
#include <memory>
#include <vector>
#include <string>

class Database {
private:
    std::unordered_map<std::string, std::unique_ptr<Table>> tables;
    // Transaction support
    bool transaction_active = false;
    std::unordered_map<std::string, std::unique_ptr<Table>> table_backups;

    void autoLoadTables(); // Added for auto-loading tables on start

public:
    Database() = default;

    void createTable(const std::string& name, const std::vector<std::string>& columns);
    void loadTable(const std::string& name);
    Table* getTable(const std::string& name);
    void showTables();
    void showTable(const std::string& name);
    void describeTable(const std::string& name);

    // Transaction methods
    void beginTransaction();
    void commitTransaction();
    void rollbackTransaction();

    void run();
};

#endif // DATABASE_HPP