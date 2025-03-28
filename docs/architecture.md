# Architecture





### Data Schema
![image](https://github.com/user-attachments/assets/9f8b5fd1-d677-4638-9989-581b04df56eb)

dim_cust
| Column Name   | Data Type      | Key         | Description                     |
| ------------- | -------------- | ----------- | ------------------------------- | 
| customer_id   | `int`          | primary key | unique identifier for customer  |
| customer_name | `varchar(100)` |             | customer name                   |
| first_name    | `varchar(50)`  |             | customer first name             |
| last_name     | `varchar(50)`  |             | customer last name              |
| create_date   | `datetime`     |             | record date of creation         |
| update_date   | `datetime`     |             | record date of update           |

dim_date
| Column Name   | Data Type      | Key         | Description                        |
| ------------- | -------------- | ----------- | ---------------------------------- | 
| date_full     | `datetime`     | primary key | unique identifier for date         |
| day_of_week   | `int`          |             | nth day of week, 1 (Mon) - 7 (Sun) |
| day_name      | `varchar(10)`  |             | day of week, Monday - Sunday       |
| day_of_month  | `int`          |             | nth day of month                   |
| day_of_year   | `int`          |             | nth day of year                    |
| week_of_year  | `int`          |             | nth week of year                   |
| month_num     | `int`          |             | nth month, 1 (Jan) - 12 (Dec)      |
| month_name    | `varchar(10)`  |             | month, January - December          |
| quarter       | `int`          |             | nth quarter of year                |
| year          | `int`          |             | year                               |
| is_weekend    | `boolean`      |             | True if day is weekend (Sat/Sun)   |

dim_emp
| Column Name         | Data Type      | Key         | Description                     |
| ------------------- | -------------- | ----------- | ------------------------------- |
| employee_id         | `int`          | primary key | unique identifier for employee  |
| employee_name       | `varchar(100)` |             | employee name                   |
| employee_first_name | `varchar(50)`  |             | employee first name             |
| employee_last_name  | `varchar(50)`  |             | employee last name              |
| create_date         | `datetime`     |             | record date of creation         |
| update_date         | `datetime`     |             | record date of update           |

dim_geo
| Column Name         | Data Type      | Key         | Description                     |
| ------------------- | -------------- | ----------- | ------------------------------- |
| state_id            | `varchar(40)`  | primary key | unique identifier for state     |
| state_code          | `varchar(2)`   |             | two-letter state code           |
| country             | `varchar(30)`  |             | country where the state sits    |
| state_name          | `varchar(50)`  |             | state full name                 |
| capital_city        | `varchar(50)`  |             | capital city of state           |
| status              | `varchar(30)`  |             | state status                    |
| iso_code            | `varchar(10)`  |             | ISO code for state (ISO 3166)   |

dim_ostatus
| Column Name        | Data Type      | Key         | Description                     |
| ------------------ | -------------- | ----------- | ------------------------------- |
| status_id          | `int`          | primary key | unique identifier order status  |
| status_name        | `varchar(15)`  |             | order status name               |
| status_description | `varchar(50)`  |             | detailed description of status  |
| create_date        | `datetime`     |             | record date of creation         |
| update_date        | `datetime`     |             | record date of update           |

dim_prod
| Column Name   | Data Type      | Key         | Description                     |
| ------------- | -------------- | ----------- | ------------------------------- |
| product_id    | `int`          | primary key | unique identifier for product   |
| product_name  | `varchar(30)`  |             | name of product                 |
| category      | `varchar(15)`  |             | product category                |
| brand         | `varchar(20)`  |             | brand of product                |
| standard_cost | `float`        |             | standard cost of product        |
| create_date   | `datetime`     |             | record date of creation         |
| update_date   | `datetime`     |             | record date of update           |

fact_orders
| Column Name   | Data Type      | Key         | Description                     |
| ------------- | -------------- | ----------- | ------------------------------- |
| order_number  | `int`          | primary key | unique identifier for order     |
| order_date    | `datetime`     | foreign key | unique identifier for date      |
| customer_id   | `int`          | foreign key | unique identifier for customer  |
| state_id      | `varchar(40)`  | foreign key | unique identifier for state     |
| product_id    | `int`          | foreign key | unique identifier for product   |
| status_id     | `int`          | foreign key | unique identifier for status    |
| employee_id   | `int`          | foreign key | unique identifier for employee  |
| unit_cost     | `float`        |             | unit cost of product            |
| unit_sales    | `float`        |             | unit sales of product           |
| quantity      | `int`          |             | quantity of product(s) in order |
| total_cost    | `float`        |             | total cost of order             |
| total_sales   | `float`        |             | total sales of order            |
| profit        | `float`        |             | profit of order                 |
| profit_margin | `float`        |             | profit margin of order          |
