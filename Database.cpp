// Database.cpp
#include "Database.hpp"
#include <sstream>
#include <algorithm>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

void Database::createTable(const std::string& name, const std::vector<std::string>& columns) {
    if (tables.find(name) != tables.end()) {
        std::cerr << "Error: Table " << name << " already exists.\n";
        return;
    }
    tables[name] = std::make_unique<Table>(name, columns);
    if (!transaction_active) {
        tables[name]->save();
    }
    std::cout << "Table " << name << " created successfully.\n";
}

void Database::loadTable(const std::string& name) {
    if (tables.find(name) != tables.end()) {
        std::cerr << "Error: Table " << name << " is already loaded.\n";
        return;
    }
    // Check if file exists
    std::string filepath = "data/" + name + ".tbl";
    if (!fs::exists(filepath)) {
        std::cerr << "Error: Table " << name << " does not exist.\n";
        return;
    }
    tables[name] = std::make_unique<Table>(name);
    std::cout << "Table " << name << " loaded successfully.\n";
}

void Database::autoLoadTables() {
    std::string data_dir = "data";
    if (fs::exists(data_dir) && fs::is_directory(data_dir)) {
        for (const auto& entry : fs::directory_iterator(data_dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".tbl") {
                std::string filename = entry.path().stem().string();
                if (tables.find(filename) == tables.end()) {
                    tables[filename] = std::make_unique<Table>(filename);
                    std::cout << "Loaded table: " << filename << "\n";
                }
            }
        }
    }
}

Table* Database::getTable(const std::string& name) {
    auto it = tables.find(name);
    if (it != tables.end()) {
        return it->second.get();
    }
    std::cerr << "Error: Table " << name << " not found.\n";
    return nullptr;
}

void Database::showTables() {
    std::cout << "Tables:\n";
    for (const auto& pair : tables) {
        std::cout << "- " << pair.first << "\n";
    }
}

void Database::showTable(const std::string& name) {
    Table* table = getTable(name);
    if (table) {
        std::vector<std::string> all_columns; // Empty vector indicates all columns
        std::vector<std::pair<std::string, std::string>> aggregates;
        table->select(all_columns, aggregates, "", "", {}, {});
    }
}

void Database::describeTable(const std::string& name) {
    Table* table = getTable(name);
    if (table) {
        std::cout << "Table: " << name << "\n";
        std::cout << "Columns:\n";
        for (const auto& col : table->getColumns()) {
            std::cout << "- " << col << "\n";
        }
    }
}

void Database::beginTransaction() {
    if (transaction_active) {
        std::cerr << "Error: Transaction already in progress.\n";
        return;
    }
    // Backup current tables
    for (auto& pair : tables) {
        table_backups[pair.first] = std::make_unique<Table>(*pair.second);
    }
    transaction_active = true;
    std::cout << "Transaction started.\n";
}

void Database::commitTransaction() {
    if (!transaction_active) {
        std::cerr << "Error: No active transaction to commit.\n";
        return;
    }
    // Save all tables
    for (auto& pair : tables) {
        pair.second->save();
    }
    table_backups.clear();
    transaction_active = false;
    std::cout << "Transaction committed.\n";
}

void Database::rollbackTransaction() {
    if (!transaction_active) {
        std::cerr << "Error: No active transaction to rollback.\n";
        return;
    }
    // Restore tables from backups
    for (auto& pair : table_backups) {
        if (tables.find(pair.first) != tables.end()) {
            tables[pair.first] = std::move(pair.second);
        }
    }
    table_backups.clear();
    transaction_active = false;
    std::cout << "Transaction rolled back.\n";
}

void Database::run() {
    // Auto load existing tables
    autoLoadTables();

    std::string input;
    std::cout << "Welcome to MiniDB! Enter SQL commands or 'exit' to quit.\n";
    while (true) {
        std::cout << "MiniDB> ";
        std::getline(std::cin, input);
        if (input.empty()) continue;

        // Exit condition
        if (input == "exit") break;

        // Convert input to uppercase for command identification
        std::stringstream ss(input);
        std::string command;
        ss >> command;
        std::string original_command = command; // Preserve original for case-sensitive parts
        std::transform(command.begin(), command.end(), command.begin(), ::toupper);

        if (command == "CREATE") {
            std::string table_keyword, table_name;
            ss >> table_keyword >> table_name;
            std::transform(table_keyword.begin(), table_keyword.end(), table_keyword.begin(), ::toupper);
            if (table_keyword != "TABLE") {
                std::cerr << "Error: Invalid syntax. Did you mean 'CREATE TABLE'? \n";
                continue;
            }
            // Parse columns
            size_t pos1 = input.find('(');
            size_t pos2 = input.find(')');
            if (pos1 == std::string::npos || pos2 == std::string::npos || pos2 <= pos1 + 1) {
                std::cerr << "Error: Invalid syntax for CREATE TABLE.\n";
                continue;
            }
            std::string cols = input.substr(pos1 + 1, pos2 - pos1 - 1);
            std::vector<std::string> columns;
            std::stringstream cols_ss(cols);
            std::string col;
            while (std::getline(cols_ss, col, ',')) {
                // Trim whitespace
                col.erase(col.begin(), std::find_if(col.begin(), col.end(), [](unsigned char ch) {
                    return !std::isspace(ch);
                }));
                col.erase(std::find_if(col.rbegin(), col.rend(), [](unsigned char ch) {
                    return !std::isspace(ch);
                }).base(), col.end());
                columns.push_back(col);
            }
            createTable(table_name, columns);
        }
        else if (command == "INSERT") {
            std::string into_keyword, table_name, values_keyword;
            ss >> into_keyword >> table_name >> values_keyword;
            std::transform(into_keyword.begin(), into_keyword.end(), into_keyword.begin(), ::toupper);
            std::transform(values_keyword.begin(), values_keyword.end(), values_keyword.begin(), ::toupper);
            if (into_keyword != "INTO" || values_keyword != "VALUES") {
                std::cerr << "Error: Invalid syntax. Use 'INSERT INTO table_name VALUES (...)'\n";
                continue;
            }
            size_t pos1 = input.find('(');
            size_t pos2 = input.find(')');
            if (pos1 == std::string::npos || pos2 == std::string::npos || pos2 <= pos1 + 1) {
                std::cerr << "Error: Invalid syntax for INSERT.\n";
                continue;
            }
            std::string vals = input.substr(pos1 + 1, pos2 - pos1 - 1);
            std::vector<std::string> values;
            std::stringstream vals_ss(vals);
            std::string val;
            while (std::getline(vals_ss, val, ',')) {
                // Trim whitespace
                val.erase(val.begin(), std::find_if(val.begin(), val.end(), [](unsigned char ch) {
                    return !std::isspace(ch);
                }));
                val.erase(std::find_if(val.rbegin(), val.rend(), [](unsigned char ch) {
                    return !std::isspace(ch);
                }).base(), val.end());
                // Remove quotes if present
                if (!val.empty() && val.front() == '\'' && val.back() == '\'') {
                    val = val.substr(1, val.size() - 2);
                }
                values.push_back(val);
            }
            Table* table = getTable(table_name);
            if (table) {
                table->insert(values);
                if (!transaction_active) {
                    table->save();
                }
                std::cout << "Record inserted into " << table_name << ".\n";
            }
        }
        else if (command == "SELECT") {
            // Extract selected columns until 'FROM' is found
            std::vector<std::string> selected_columns;
            std::vector<std::pair<std::string, std::string>> aggregates; // aggregate function and column
            std::string token;

            while (ss >> token &&
                   token != "FROM" &&
                   token != "from" &&
                   token != "From") {
                // Handle cases where columns are separated by commas
                if (token.back() == ',') {
                    token.pop_back(); // Remove trailing comma
                }
                // Check for aggregate functions
                size_t pos = token.find('(');
                if (pos != std::string::npos && token.back() == ')') {
                    std::string func = token.substr(0, pos);
                    std::string arg = token.substr(pos + 1, token.size() - pos - 2);
                    std::transform(func.begin(), func.end(), func.begin(), ::toupper);
                    if (func == "COUNT") {
                        aggregates.emplace_back("COUNT", arg);
                    }
                    else {
                        std::cerr << "Error: Unsupported aggregate function '" << func << "'.\n";
                        aggregates.clear();
                        break;
                    }
                }
                else {
                    selected_columns.push_back(token);
                }
            }

            // Check if 'FROM' keyword was found
            if (!(token == "FROM" || token == "from" || token == "From")) {
                std::cerr << "Error: Invalid syntax. Missing 'FROM'.\n";
                continue;
            }

            // Extract table name
            std::string table_name;
            ss >> table_name;
            if (table_name.empty()) {
                std::cerr << "Error: Missing table name after 'FROM'.\n";
                continue;
            }

            // Initialize variables for WHERE, ORDER BY, GROUP BY clauses
            std::string clause;
            std::string where_column, where_value;
            std::vector<std::pair<std::string, std::string>> order_by; // column and direction
            std::vector<std::string> group_by;

            while (ss >> clause) {
                std::string upper_clause = clause;
                std::transform(upper_clause.begin(), upper_clause.end(), upper_clause.begin(), ::toupper);
                if (upper_clause == "WHERE") {
                    ss >> where_column >> where_value;
                    // Remove potential semicolon at the end of where_value
                    if (!where_value.empty() && where_value.back() == ';') {
                        where_value.pop_back();
                    }
                    // Remove quotes if present
                    if (!where_value.empty() && where_value.front() == '\'' && where_value.back() == '\'') {
                        where_value = where_value.substr(1, where_value.size() - 2);
                    }
                }
                else if (upper_clause == "ORDER") {
                    std::string by;
                    ss >> by;
                    std::transform(by.begin(), by.end(), by.begin(), ::toupper);
                    if (by != "BY") {
                        std::cerr << "Error: Invalid syntax after 'ORDER'. Did you mean 'ORDER BY'? \n";
                        break;
                    }
                    std::string order_col, direction = "ASC";
                    ss >> order_col;
                    if (!(ss >> direction)) {
                        direction = "ASC";
                    }
                    else {
                        std::transform(direction.begin(), direction.end(), direction.begin(), ::toupper);
                        if (direction != "ASC" && direction != "DESC") {
                            ss.unget(); // Put back the non-direction token if it's not direction
                            direction = "ASC";
                        }
                    }
                    order_by.emplace_back(order_col, direction);
                }
                else if (upper_clause == "GROUP") {
                    std::string by;
                    ss >> by;
                    std::transform(by.begin(), by.end(), by.begin(), ::toupper);
                    if (by != "BY") {
                        std::cerr << "Error: Invalid syntax after 'GROUP'. Did you mean 'GROUP BY'? \n";
                        break;
                    }
                    std::string group_col;
                    ss >> group_col;
                    group_by.emplace_back(group_col);
                }
                else {
                    std::cerr << "Error: Unrecognized clause '" << clause << "'.\n";
                    break;
                }
            }

            // Handle '*' to select all columns
            if (selected_columns.size() == 1 && selected_columns[0] == "*") {
                selected_columns.clear(); // Passing an empty vector will indicate selecting all columns
            }

            // Retrieve the table and perform the select operation
            Table* table = getTable(table_name);
            if (table) {
                table->select(selected_columns, aggregates, where_column, where_value, order_by, group_by);
            }
        }
        else if (command == "UPDATE") {
            std::string table_name, set_keyword;
            ss >> table_name >> set_keyword;
            std::transform(set_keyword.begin(), set_keyword.end(), set_keyword.begin(), ::toupper);
            if (set_keyword != "SET") {
                std::cerr << "Error: Invalid syntax. Did you mean 'SET'? \n";
                continue;
            }
            std::string set_column, equal_sign, set_value;
            ss >> set_column >> equal_sign >> set_value;
            if (equal_sign != "=") {
                std::cerr << "Error: Invalid syntax for SET. Expected '='.\n";
                continue;
            }

            // Remove quotes if present
            if (!set_value.empty() && set_value.front() == '\'' && set_value.back() == '\'') {
                set_value = set_value.substr(1, set_value.size() - 2);
            }

            // Handle optional WHERE clause
            std::string clause;
            std::string where_column, where_value;
            if (ss >> clause) {
                std::string upper_clause = clause;
                std::transform(upper_clause.begin(), upper_clause.end(), upper_clause.begin(), ::toupper);
                if (upper_clause == "WHERE") {
                    ss >> where_column >> where_value;
                    // Remove potential semicolon at the end of where_value
                    if (!where_value.empty() && where_value.back() == ';') {
                        where_value.pop_back();
                    }
                    // Remove quotes if present
                    if (!where_value.empty() && where_value.front() == '\'' && where_value.back() == '\'') {
                        where_value = where_value.substr(1, where_value.size() - 2);
                    }
                }
                else {
                    std::cerr << "Error: Unrecognized clause '" << clause << "' in UPDATE.\n";
                    continue;
                }
            }

            Table* table = getTable(table_name);
            if (table) {
                table->update(set_column, set_value, where_column, where_value);
                if (!transaction_active) {
                    table->save();
                }
            }
        }
        else if (command == "DELETE") {
            std::string from_keyword, table_name;
            ss >> from_keyword >> table_name;
            std::transform(from_keyword.begin(), from_keyword.end(), from_keyword.begin(), ::toupper);
            if (from_keyword != "FROM") {
                std::cerr << "Error: Invalid syntax. Did you mean 'DELETE FROM'? \n";
                continue;
            }

            // Handle optional WHERE clause
            std::string clause;
            std::string where_column, where_value;
            if (ss >> clause) {
                std::string upper_clause = clause;
                std::transform(upper_clause.begin(), upper_clause.end(), upper_clause.begin(), ::toupper);
                if (upper_clause == "WHERE") {
                    ss >> where_column >> where_value;
                    // Remove potential semicolon at the end of where_value
                    if (!where_value.empty() && where_value.back() == ';') {
                        where_value.pop_back();
                    }
                    // Remove quotes if present
                    if (!where_value.empty() && where_value.front() == '\'' && where_value.back() == '\'') {
                        where_value = where_value.substr(1, where_value.size() - 2);
                    }
                }
                else {
                    std::cerr << "Error: Unrecognized clause '" << clause << "' in DELETE.\n";
                    continue;
                }
            }

            Table* table = getTable(table_name);
            if (table) {
                table->deleteRecords(where_column, where_value);
                if (!transaction_active) {
                    table->save();
                }
            }
        }
        else if (command == "SHOW") {
            std::string target;
            ss >> target;
            std::transform(target.begin(), target.end(), target.begin(), ::toupper);
            if (target == "TABLES") {
                showTables();
            }
            else {
                // Assume it's a table name
                showTable(target);
            }
        }
        else if (command == "DESCRIBE") {
            std::string table_name;
            ss >> table_name;
            if (table_name.empty()) {
                std::cerr << "Error: Missing table name for DESCRIBE.\n";
                continue;
            }
            describeTable(table_name);
        }
        else if (command == "BEGIN") {
            std::string transaction_keyword;
            ss >> transaction_keyword;
            std::transform(transaction_keyword.begin(), transaction_keyword.end(), transaction_keyword.begin(), ::toupper);
            if (transaction_keyword != "TRANSACTION" && transaction_keyword != "TRANSACTION;") {
                std::cerr << "Error: Invalid syntax. Use 'BEGIN TRANSACTION'.\n";
                continue;
            }
            beginTransaction();
        }
        else if (command == "COMMIT") {
            commitTransaction();
        }
        else if (command == "ROLLBACK") {
            rollbackTransaction();
        }
        else {
            std::cerr << "Error: Unrecognized command.\n";
        }
    }
}