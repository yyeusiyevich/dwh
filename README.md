# Data Warehousing
![python_logo.jpg](logo.jpg)

## Description :key:
This project showcases a robust data warehouse that centralizes, standardizes, and enhances data sourced from Iowa Liquor sales, encapsulating data from 2022 to the present. The overarching objective of this undertaking is to build a powerful decision-support tool that forms a comprehensive platform for in-depth business analysis and strategic decision-making.

Built with a hybrid data modeling strategy, the data warehouse assures high levels of flexibility, usability, and integrity of data. Initially, raw data is captured in a Staging Area, a preliminary repository that allows for data cleaning and standardization. This data is subsequently transitioned into the 3NF layer, employing a normalized design structure to eliminate data redundancy and preserve data consistency. The data is ultimately housed in the dimensional layer, striking a balance between simplicity and structural detail, thereby facilitating both spontaneous queries and more intricate analytics. Both Star and Snowflake schemas are utilized and visualized using Lucidchart, allowing for a clear and efficient representation of data structures.

The project integrates various types of Slowly Changing Dimensions (SCD) - Type 2, 5, and 6. This unique approach effectively captures historical data changes, attribute modifications, and supports hybrid changes, thereby adding a higher level of analytical depth and enabling a comprehensive view of business evolution over time.

Data loading into the warehouse leverages efficient and flexible strategies, such as data partitioning and load-date tracking. These include full and incremental loading approaches, facilitating both total dataset reloads and smaller, more frequent updates. This strategy optimizes resource usage and ensures data remains timely and relevant.

The code for this project, including functions and procedures, is developed in PostgreSQL using the PL/pgSQL procedural language, ensuring compatibility and performance. The project includes a rigorous testing environment to ensure data quality within the warehouse, supplemented with a range of custom analytical functions to enable actionable insights and support informed decision-making.

This data warehouse project serves as a comprehensive platform, transforming raw Iowa Liquor sales data into actionable business intelligence. It emphasizes data quality, optimizes data management practices, and offers sophisticated analytics capabilities. This system provides an invaluable tool for both operational management and strategic decision-making processes in the beverage alcohol industry.

## Programming Languages :mortar_board:
SQL (PostgreSQL, PLpgSQL).

## Used Tools :ballot_box_with_check:
Lucidchart, Dbeaver, Power Point.

## Tags :label:
SQL, PostgreSQL, Data Modelling, Data Warehousing, Iowa Liquor Sales, ERD, Data Analysis, Star schema, Snowflaking, Inmon, Kimball, Slowly Changing Dimensions (SCD).

## Project Status :black_square_button:
_Completed_ 

