# Creating_own_database

A lightweight C++ database implementation featuring SQL-like operations and transaction management.

## Features

- **Table Management**
  - Create and describe tables with custom columns
  - Persistent storage of table data
  - Dynamic table operations
  - Supports data persistence through file I/O
  
- **Data Operations**
  - INSERT records into tables
  - SELECT data with column filtering
  - UPDATE existing records
  - DELETE records
  - Aggregate functions support
  - WHERE clause filtering
  - ORDER BY functionality
  - GROUP BY operations

- **Transaction Support**
  - BEGIN TRANSACTION
  - COMMIT changes
  - ROLLBACK to previous state
  - ACID compliance for data integrity

## Commands

```sql
CREATE TABLE tablename (column1, column2, ...)
INSERT INTO tablename VALUES (value1, value2, ...)
SELECT columns FROM tablename [WHERE condition]
UPDATE tablename SET column=value [WHERE condition]
DELETE FROM tablename [WHERE condition]
BEGIN TRANSACTION
COMMIT
ROLLBACK
DESCRIBE tablename
exit to quit
```

## Requirements

### Windows

- Visual Studio 2019 or later with C++17 support
- CMake 3.15+
- MSVC compiler

### Linux/Unix

- GCC 8+ or Clang 7+
- CMake 3.15+
- Make

### macOS

- Xcode Command Line Tools
- GCC 8+ or Clang 7+
- CMake 3.15+
- Make

## Platform-Specific Build Instructions

### Windows
```bash
mkdir build
cd build
cmake ..
msbuild Creating_own_database.sln
```

### Linux/Unix
```bash
mkdir build
cd build
cmake ..
make
```

### macOS
```bash
mkdir build
cd build
cmake ..
make
```

## Technical Details

### Core Components

1. **Database Class**
   - Main interface for database operations
   - Manages tables and transactions
   - Handles command parsing and execution

2. **Table Class**
   - Manages individual table structure and data
   - Handles CRUD operations

### Data Storage

- Tables are stored in a `data` directory
- Each table maintains its own file

## Usage

1. Compile and run the program
2. Use the interactive command prompt:
```bash
MiniDB> CREATE TABLE users (id, name, email)
MiniDB> INSERT INTO users VALUES (1, "John", "john@email.com")
MiniDB> SELECT * FROM users
```

## Example Commands

```sql
-- Create the "students" table
CREATE TABLE students (id, name, rollno)

-- Insert records into the "students" table
INSERT INTO students VALUES (1, 'Advait', 'B22CS004')
INSERT INTO students VALUES (2, 'Qazi', 'B22CS087')
INSERT INTO students VALUES (3, 'Aditya', 'B22CS103')

-- Display all records from the "students" table
SELECT * FROM students

-- Display specific columns with a condition
SELECT name, rollno FROM students WHERE id 2

-- Update a record in the "students" table
UPDATE students SET name = 'Talha' WHERE id 2

-- Delete a specific record from the "students" table
DELETE FROM students WHERE id 3

-- Display all records ordered by name in descending order
SELECT * FROM students ORDER BY name DESC

-- Describe the structure of the "students" table
DESCRIBE students

-- Show all tables in the database
SHOW TABLES

-- Begin a transaction
BEGIN TRANSACTION

-- Insert a new record within the transaction
INSERT INTO students VALUES (4, 'Aditya', 'B22ES003')

-- Delete a record within the transaction
DELETE FROM students WHERE id 1

-- Rollback the transaction (undo the insert and delete)
ROLLBACK

-- Verify the state of the "students" table after rollback
SELECT * FROM students

-- Begin another transaction
BEGIN TRANSACTION

-- Insert a new record within the transaction
INSERT INTO students VALUES (5, 'Aditya', 'B22ES003')

-- Update a record within the transaction
UPDATE students SET rollno = 'B22CS103' WHERE name 'Aditya'

-- Commit the transaction (save the update)
COMMIT

-- Verify the final state of the "students" table
SELECT * FROM students

-- Group by name and count occurrences of each name
SELECT name, COUNT(*) FROM students GROUP BY name
```

#### To exit the program, use the command `exit`.