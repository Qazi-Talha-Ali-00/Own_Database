// Record.hpp
#ifndef RECORD_HPP
#define RECORD_HPP

#include <vector>
#include <string>

class Record {
public:
    std::vector<std::string> fields;

    Record() = default;
    Record(const std::vector<std::string>& fields) : fields(fields) {}
};

#endif // RECORD_HPP