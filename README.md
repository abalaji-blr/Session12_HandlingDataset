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

import csv

def read_file(file_name):
    with open(file_name) as f:
        rows = csv.reader(f, delimiter=',', quotechar='"')
        yield from rows
    
from itertools import islice

 rows = read_file('personal_info.csv')
 for row in islice(rows, 5):
     print(row)
     
As you can see, the data is already separated into a list containing the individual fields - but of course they are all just strings.
