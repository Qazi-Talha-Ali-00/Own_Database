// main.cpp
#include "Database.hpp"
#include <filesystem>
#include <iostream>

int main() {
    // Create data directory if it doesn't exist
    std::string data_dir = "data";
    if (!std::filesystem::exists(data_dir)) {
        std::filesystem::create_directory(data_dir);
    }

    Database db;
    db.run();
    return 0;
}