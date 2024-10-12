# Dynamic ETL DAG: MySQL to PostgreSQL

## Table of Contents
- [Overview](#overview)

## Overview

This project implements a **Dynamic ETL (Extract, Transform, Load)** pipeline using **Apache Airflow** to transfer data from **MySQL** to **PostgreSQL**. The DAG is designed to handle **exactly 5 tables** dynamically, ensuring scalability and maintainability. It runs **every 2 hours** between **9:15 AM** and **9:15 PM** on the **first and third Fridays** of each month.

### Key Features

- **Dynamic Task Creation:** Automatically creates ETL tasks for each specified table.
- **Configuration Management:** Utilizes a JSON configuration file for easy adjustments.
- **Selective Scheduling:** Runs only on the first and third Fridays, at specified times.
- **Error Handling & Notifications:** Sends email alerts on failures.
- **Logging:** Comprehensive logging for monitoring and debugging.



