// Table.hpp
#ifndef TABLE_HPP
#define TABLE_HPP

#include "Record.hpp"
#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <functional>

class Table {
private:
    std::string name;
    std::vector<std::string> columns;
    std::vector<Record> records;
    std::string filepath;

public:
    Table(const std::string& name, const std::vector<std::string>& columns);
    Table(const std::string& name); // Load existing table

    void insert(const std::vector<std::string>& fields);
    void select(const std::vector<std::string>& select_columns, 
               const std::vector<std::pair<std::string, std::string>>& aggregates,
               const std::string& where_column = "", 
               const std::string& where_value = "",
               const std::vector<std::pair<std::string, std::string>>& order_by = {},
               const std::vector<std::string>& group_by = {});
    void update(const std::string& set_column, const std::string& set_value, 
               const std::string& where_column = "", 
               const std::string& where_value = "");
    void deleteRecords(const std::string& where_column = "", const std::string& where_value = "");

    void save();
    void load();
    const std::string& getName() const { return name; }
    const std::vector<std::string>& getColumns() const { return columns; }

    // For transaction backup
    Table(const Table& other) : name(other.name), columns(other.columns), records(other.records), filepath(other.filepath) {}
};

#endif // TABLE_HPP