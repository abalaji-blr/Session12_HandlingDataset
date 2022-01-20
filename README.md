# Session12: Generators and Handling Dataset

### About Data

For this project, you have 4 files containing information about persons.

The files are:

* personal_info.csv -   personal information such as name, gender, etc. (one row per person)

* vehicles.csv -   what vehicle people own (one row per person)

* employment.csv -   where a person is employed (one row per person)

* update_status.csv -   when the person's data was created and last updated

Each file contains a key, SSN, which *uniquely* identifies a person.

This key is present in all four files.

You are guaranteed that the same SSN value is present in *every* file, and that it only appears *once per file*.

In addition, the files are all sorted by SSN, i.e. the SSN values appear in the same order in each file.

### Objectives

#### Goal 1

Your first task is to create iterators for each of the four files that contained cleaned up data, of the correct type (e.g. string, int, date, etc), and represented by a named tuple.

For now these four iterators are just separate, independent iterators.

##### Goal 2

Create a single iterable that combines all the columns from all the iterators.

The iterable should yield named tuples containing all the columns.

Make sure that the SSN's across the files match!



All the files are guaranteed to be in SSN sort order, and every SSN is unique, and every SSN appears in every file.

Make sure the SSN is not repeated 4 times - one time per row is enough!

##### Goal 3

Next, you want to identify any stale records, where stale simply means the record has not been updated since 3/1/2017 (e.g. last update date < 3/1/2017). Create an iterator that only contains current records (i.e. not stale) based on the `last_updated` field from the `status_update` file.

##### Goal 4

Find the largest group of car makes for each gender.

Possibly more than one such group per gender exists (equal sizes).

##### Hints

You will not be able to use a simple split approach here, as I explain in the video.

Instead you should use the `csv` module and the `reader` function.



Here's a simple example of how to use it - you will need to expand on this for your project goals, but this is a good starting point.

```python
import csv

def read_file(file_name):
    with open(file_name) as f:
        rows = csv.reader(f, delimiter=',', quotechar='"')
        yield from rows
```

```python
from itertools import islice

 rows = read_file('personal_info.csv')
 for row in islice(rows, 5):
     print(row)
```


As you can see, the data is already separated into a list containing the individual fields - but of course they are all just strings.

## Goal 1 - Create Iterator For Each File

### Build Generic Iterator

```python
def read_file_gen(filename, row_parser, nt_name = 'DATA'):
  '''
  Inputs: filename, row value parser
  Outputs: Namedtuple
  '''
  with open(filename) as f:
    rows = csv.reader(f,delimiter =',', quotechar = '"')

    hdr = next(rows)
    #build namedtuple
    nt_name = namedtuple(nt_name, hdr)

    for r in rows:
      row_value = [func(val) for func, val in zip(row_parser, r)]
      yield nt_name._make(row_value)
```

```python
#define the row value parsers
row_parser = (parse_ssn,
                  parse_string,
                  parse_string,
                  parse_string,
                  parse_string)
  
gen = read_file_gen(dir_loc+ per_file, row_parser, 'PER_DATA')

for row in islice(gen, 5):
  print(row)
```

```python
PER_DATA(ssn=100539824, first_name='Sebastiano', last_name='Tester', gender='Male', language='Icelandic')
PER_DATA(ssn=101714702, first_name='Cayla', last_name='MacDonagh', gender='Female', language='Lao')
PER_DATA(ssn=101840356, first_name='Nomi', last_name='Lipprose', gender='Female', language='Yiddish')
PER_DATA(ssn=104220928, first_name='Justinian', last_name='Kunzelmann', gender='Male', language='Dhivehi')
PER_DATA(ssn=104847144, first_name='Claudianus', last_name='Brixey', gender='Male', language='Afrikaans')
```

```python
#['employer', 'department', 'employee_id', 'ssn']
#['Stiedemann-Bailey', 'Research and Development', '29-0890771', '100-53-9824']
emp_row_parser = (parse_string,
                  parse_string,
                  parse_emp_id,
                  parse_ssn)

emp_gen = read_file_gen(dir_loc+emp_file, emp_row_parser, 'EMP_DATA')

for row in islice(emp_gen, 5):
  print(row)
```

```python
EMP_DATA(employer='Stiedemann-Bailey', department='Research and Development', employee_id=290890771, ssn=100539824)
EMP_DATA(employer='Nicolas and Sons', department='Sales', employee_id=416841359, ssn=101714702)
EMP_DATA(employer='Connelly Group', department='Research and Development', employee_id=987952860, ssn=101840356)
EMP_DATA(employer='Upton LLC', department='Marketing', employee_id=569817552, ssn=104220928)
EMP_DATA(employer='Zemlak-Olson', department='Business Development', employee_id=462886707, ssn=104847144)
```

## Goal 2: Create a single iterable for all files.

Create a single iterable that combines all the columns from all the iterators.

The iterable should yield named tuples containing all the columns. Make sure that the SSN's across the files match!

```python
def read_file_gen_return_tuple(filename, row_parser):
  '''
  inputs: file-name, row_parser
  output: list of tuples, [(col-name, val), (col-name2, val2) ... ]
  '''
  with open(filename) as f:
    rows = csv.reader(f,delimiter =',', quotechar = '"')
    hdr = next(rows)
    
    for r in rows:
      row_value = [func(val) for func, val in zip(row_parser, r)]
      yield list(zip(hdr, row_value))
```

```python
gen1 = read_file_gen_return_tuple(dir_loc+ per_file, row_parser)
gen2 = read_file_gen_return_tuple(dir_loc+emp_file, emp_row_parser)
gen3 = read_file_gen_return_tuple(dir_loc+veh_file, veh_row_parser)
gen4 = read_file_gen_return_tuple(dir_loc+status_file, stat_row_parser)

single_gen = create_single_gen(gen1, gen2, gen3, gen4)

for row in islice(single_gen, 5):
  print(row)
```

```python
CombinedData(employer='Kohler, Bradtke and Davis', last_name='Aggett', created=datetime.datetime(2016, 7, 23, 17, 58, 35), ssn=105275541, first_name='Federico', employee_id=800975518, model_year=2001, vehicle_model='Mustang', vehicle_make='Ford', language='Chinese', gender='Male', department='Support', last_updated=datetime.datetime(2017, 7, 24, 8, 58, 52))

CombinedData(employer='Roberts, Torphy and Dach', last_name='McAvey', created=datetime.datetime(2016, 12, 15, 5, 46, 43), ssn=105857486, first_name='Angelina', employee_id=774895332, model_year=2008, vehicle_model='300', vehicle_make='Chrysler', language='Punjabi', gender='Female', department='Human Resources', last_updated=datetime.datetime(2018, 2, 14, 11, 32, 39))
```

## Goal 3: Create Iterator To Ignore Stale Records

Next, you want to identify any stale records, where stale simply means the record has not been updated since 3/1/2017 (e.g. last update date < 3/1/2017). Create an iterator that only contains current records (i.e. not stale) based on the `last_updated` field from the `status_update` file.

```python
def is_record_stale(complete_row):
  '''
  The record is stale if not updated on or after
  the reference date (Mar/1/2017)
  '''
  # yyyy/mm/dd
  reference_date = datetime(2017, 3, 1).date()
  for item in complete_row:
    key = item[0]
    value = item[1]
    if key == 'last_updated':
      #print(value.date())
      if value.date() < reference_date:
        return True
      else:
        return False
```

```python
single_gen = create_single_gen(gen1, gen2, gen3, gen4)
for row in islice(single_gen, 5):
  print(row)
```

```python
CombinedData(department='Accounting', employee_id=315735282, vehicle_make='Volkswagen', vehicle_model='Touareg', created=datetime.datetime(2016, 5, 16, 21, 21, 36), first_name='Martino', ssn=106363293, last_updated=datetime.datetime(2017, 3, 18, 18, 24, 17), gender='Male', language='Tok Pisin', employer='Leffler-Hahn', last_name='Tregoning', model_year=2008)

CombinedData(department='Marketing', employee_id=339146042, vehicle_make='Ford', vehicle_model='Mustang', created=datetime.datetime(2016, 9, 12, 22, 50, 5), first_name='Amberly', ssn=110843641, last_updated=datetime.datetime(2017, 9, 4, 19, 2, 3), gender='Female', language='Papiamento', employer='Lueilwitz LLC', last_name='Huws', model_year=1990)
```

## Goal 4: Find the largest group of car makes for each gender.

```python
print('------Vehicle Make of Females----------')
pprint.pprint(female_veh_dict)
print(f'Total number of vehicles {sum(female_veh_dict.values())}')

print('-------Vehicle Make of Males----------')
pprint.pprint(male_veh_dict)
print(f'Total number of vehicles {sum(male_veh_dict.values())}')
```

```python
------Vehicle Make of Females----------
{'Acura': 9,
 'Aston Martin': 2,
 'Audi': 13,
 'Austin': 1,
 'BMW': 12,
 'Bentley': 4,
 'Bugatti': 1,
 'Buick': 11,
 'Cadillac': 6,
 'Chevrolet': 42,
 'Chrysler': 6,
 'Dodge': 17,
 'Eagle': 1,
 'Ford': 42,
 'GMC': 22,
 'Geo': 1,
 'Honda': 8,
 'Hyundai': 4,
 'Infiniti': 9,
 'Isuzu': 3,
 'Jaguar': 3,
 'Jeep': 5,
 'Kia': 9,
 'Lamborghini': 2,
 'Land Rover': 8,
 'Lexus': 15,
 'Lincoln': 4,
 'Lotus': 5,
 'Mazda': 13,
 'Mercedes-Benz': 17,
 'Mercury': 5,
 'Mitsubishi': 22,
 'Morgan': 1,
 'Nissan': 12,
 'Oldsmobile': 8,
 'Panoz': 1,
 'Plymouth': 3,
 'Pontiac': 14,
 'Porsche': 3,
 'Rolls-Royce': 1,
 'Saab': 3,
 'Saturn': 3,
 'Scion': 2,
 'Subaru': 6,
 'Suzuki': 12,
 'Toyota': 20,
 'Volkswagen': 10,
 'Volvo': 13}
Total number of vehicles 434
-------Vehicle Make of Males----------
{'Acura': 7,
 'Aptera': 1,
 'Aston Martin': 3,
 'Audi': 14,
 'Austin': 1,
 'BMW': 12,
 'Bentley': 3,
 'Buick': 13,
 'Cadillac': 9,
 'Chevrolet': 30,
 'Chrysler': 3,
 'Corbin': 1,
 'Daewoo': 1,
 'Dodge': 22,
 'Eagle': 1,
 'Ford': 40,
 'GMC': 28,
 'Geo': 2,
 'Honda': 9,
 'Hyundai': 8,
 'Infiniti': 7,
 'Isuzu': 3,
 'Jaguar': 4,
 'Jeep': 7,
 'Jensen': 1,
 'Kia': 5,
 'Lamborghini': 4,
 'Land Rover': 3,
 'Lexus': 6,
 'Lincoln': 5,
 'Lotus': 5,
 'Maserati': 3,
 'Maybach': 2,
 'Mazda': 13,
 'Mercedes-Benz': 19,
 'Mercury': 11,
 'Mitsubishi': 28,
 'Nissan': 6,
 'Oldsmobile': 5,
 'Panoz': 2,
 'Plymouth': 4,
 'Pontiac': 11,
 'Porsche': 4,
 'Rolls-Royce': 1,
 'Saab': 8,
 'Saturn': 3,
 'Scion': 1,
 'Smart': 1,
 'Subaru': 8,
 'Suzuki': 2,
 'Toyota': 21,
 'Volkswagen': 16,
 'Volvo': 10}
Total number of vehicles 437
```

