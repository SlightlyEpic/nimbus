# NimbusDB CLI Example Commands (22 Examples)

This document provides a comprehensive guide to the command-line interface (CLI) for NimbusDB. The commands below demonstrate database operations, focusing on the correct pattern for stable data modification and indexing.

| Command Category | Data Types |
| :--- | :--- |
| **Supported Types** | `INT` (maps to U32), `VARCHAR` |

***

## I. Schema Definition and Indexing (The Stable Pattern)

To ensure reliable `UPDATE` and `DELETE` operations, an index on the filter column (`id`) is crucial to avoid sequential scan issues when modifying the heap page structure.

| Command # | Command | Description |
| :--- | :--- | :--- |
| **1** | `CREATE TABLE Users (id INT, name VARCHAR, age INT);` | Creates the primary `Users` table. |
| **2** | `CREATE INDEX idx_id ON Users(id);` | **CRITICAL:** Creates an index on the `id` column. This forces a stable **Index Scan** for DML operations filtering by `id` (e.g., in steps 14 & 15). |
| **3** | `CREATE TABLE Products (sku INT, item_name VARCHAR, price INT);` | Creates a second table for variety. |
| **4** | `CREATE INDEX idx_sku ON Products(sku);` | Creates an index on the `Products.sku` column. |
| **5** | `SHOW TABLES;` | Lists all tables using the standard SQL command (this command is now working). |

***

## II. Data Insertion and Retrieval (DML)

These commands populate the tables and demonstrate basic querying techniques.

| Command # | Command | Operation |
| :--- | :--- | :--- |
| **6** | `INSERT INTO Users (id, name, age) VALUES (1, 'Alice', 30);` | Inserts the first record. |
| **7** | `INSERT INTO Users (id, name, age) VALUES (2, 'Bob', 25);` | Inserts the second record (Target for Update). |
| **8** | `INSERT INTO Users (id, name, age) VALUES (3, 'Charlie', 35);` | Inserts the third record (Target for Delete). |
| **9** | `INSERT INTO Users (id, name, age) VALUES (4, 'David', 25);` | Inserts the fourth record. |
| **10** | `SELECT * FROM Users;` | Retrieves all columns and all rows (Full Table Scan). |
| **11** | `SELECT name, age FROM Users;` | Retrieves only the specified columns (Projection). |
| **12** | `SELECT name FROM Users WHERE id = 2;` | Retrieves the name using the stable Index Scan on `idx_id`. |
| **13** | `SELECT * FROM Users WHERE age = 25;` | Retrieves all records where `age` equals 25 (Sequential Scan with Filter). |

***

## III. Data Modification and Cleanup

These commands demonstrate the corrected `UPDATE`/`DELETE` logic and clean up the database.

| Command # | Command | Operation |
| :--- | :--- | :--- |
| **14** | `UPDATE Users SET name = 'Robert' WHERE id = 2;` | **STABLE UPDATE:** Changes 'Bob' to 'Robert'. This is now stable and persists across sessions due to the required indexing and corrected heap chain logic in the system. |
| **15** | `SELECT * FROM Users WHERE id = 2;` | **VERIFY:** Confirms the update and persistence of 'Robert' (1 row returned). |
| **16** | `DELETE FROM Users WHERE name = 'Charlie';` | Deletes the row for 'Charlie'. |
| **17** | `SELECT * FROM Users;` | **VERIFY:** Confirms the deletion (3 rows remaining: Alice, Robert, David). |
| **18** | `INSERT INTO Products (sku, item_name, price) VALUES (1001, 'Laptop', 1200);` | Inserts a data point into the `Products` table. |
| **19** | `SELECT item_name FROM Products WHERE sku = 1001;` | Retrieves item name using the **Index Scan** on `idx_sku`. |
| **20** | `DROP TABLE Products;` | Permanently removes the `Products` table and its associated index. |
| **21** | `DROP TABLE Users;` | Permanently removes the `Users` table and its associated index (`idx_id`). |
| **22** | `.tables` | Utility command to confirm both tables have been dropped from the system. |

***

## IV. Utilities and Exit Instructions

These commands are crucial for managing the session and environment.

| Command | Description |
| :--- | :--- |
| **`.exit`** | Safely shuts down the NimbusDB process, flushing all dirty pages to the disk file (`nimbus.db`) to ensure data persistence. |
| **`.help`** | Displays the help menu for all supported CLI and SQL commands. |