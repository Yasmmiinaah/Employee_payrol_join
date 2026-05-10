# Employee_payrol_join
````markdown
# Employee Payroll Join using Hadoop MapReduce

## Project Overview
This project implements a Reduce-Side Join using Hadoop MapReduce to join two datasets:

1. **Employee Directory File**
   Contains employee details:
   - employeeId
   - firstName
   - lastName
   - department

2. **Payroll File**
   Contains monthly payroll transactions:
   - payrollId
   - employeeId
   - month
   - baseSalary
   - bonus

The project joins both files using employeeId and generates enriched payroll output including:
- Employee full name
- Department
- Month
- Total Pay
- Maximum Pay across all months for each employee

---

## Input Format

### Employee File
```csv
employeeId,firstName,lastName,department
EMP01,Nour,Hassan,Engineering
EMP02,Sara,Ali,HR
````

### Payroll File

```csv
payrollId,employeeId,month,baseSalary,bonus
PR001,EMP01,Jan,8000,500
PR002,EMP01,Feb,8200,500
```

---

## Output Format

```text
employeeId    fullName,department,month,totalPay,maxPay
```

Example:

```text
EMP01    Nour Hassan,Engineering,Jan,8500,8700
EMP01    Nour Hassan,Engineering,Feb,8700,8700
```

Where:

* totalPay = baseSalary + bonus
* maxPay = maximum totalPay for the same employee across all months

---

## Project Components

### 1. EmployeeMapper

Processes employee records and emits:

```text
employeeId -> emp~firstName,lastName,department
```

Example:

```text
EMP01 -> emp~Nour,Hassan,Engineering
```

---

### 2. PayrollMapper

Processes payroll records and emits:

```text
employeeId -> pay~month,baseSalary,bonus
```

Example:

```text
EMP01 -> pay~Jan,8000,500
```

Features:

* Skips header row
* Skips malformed lines
* Skips invalid salary/bonus values
* Uses Hadoop counters for invalid records

---

### 3. PayrollReducer

Performs reduce-side join by:

* Reading employee metadata
* Reading payroll transactions
* Calculating totalPay
* Tracking maxPay for each employee
* Emitting enriched payroll records

If employee is missing:

```text
UNKNOWN EMPLOYEE,UNKNOWN
```

Example:

```text
EMP99    UNKNOWN EMPLOYEE,UNKNOWN,Jan,7500,7500
```

---

### 4. Driver Class

Responsible for:

* Configuring MapReduce job
* Setting input/output paths
* Registering mappers using MultipleInputs
* Setting reducer
* Passing tags:

  * emp~
  * pay~
* Setting reducers count to 1

---

## Hadoop Features Used

* Hadoop MapReduce
* MultipleInputs
* Reduce-Side Join
* Hadoop Counters
* Input Validation
* Error Handling

---

## Why Combiner Was Not Used

Combiner was not used because this project performs a reduce-side join.

A combiner is only suitable for associative and commutative operations such as:

* sum
* count
* min
* max

This reducer requires complete grouped records for each employeeId to correctly join employee metadata with payroll transactions and compute maxPay.

Therefore, combiner is not appropriate for this task.

---

## Error Handling

The project handles:

* Empty lines
* Header rows
* Missing columns
* Invalid salary/bonus values
* Unknown employee IDs

Invalid payroll lines are skipped safely.

---

## Running the Project

### Compile

```bash
javac -classpath `hadoop classpath` -d classes *.java
jar -cvf payrolljoin.jar -C classes/ .
```

### Run

```bash
hadoop jar payrolljoin.jar Driver /input/employees.csv /input/payroll.csv /output/payroll_join_output
```

### View Output

```bash
hdfs dfs -cat /output/payroll_join_output/part-r-00000
```

---

## Sample Files Included

* sample employee input
* sample payroll input
* sample output

---

## Author

* Yasminna Ramadan
* ibrahim galal
* shahd

