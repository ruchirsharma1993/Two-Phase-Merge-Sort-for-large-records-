# Two-Phase-Merge-Sort-for-large-records-

The size of the file is too big to be held in the memory during sorting. This algorithm minimizes the number of disk accesses and improves the sorting performance.

Sorting algorithm consists of two phases: Sort phase followed by Merge phase

------------------------------------------------
             Input Format: 
 ------------------------------------------------
metadata.txt (considered in the same folder as your executable file) about the column size. 
<column name 1>,<size of the column> 
<column name 2>,<size of the column> <column name 3>,<size of the column> 
...... 
<column name n>,<size of the column> 

input.txt 
Containing the records with the column values separated by the space. 
All the values will be in the string only. 

------------------------------------------------
         Command Line Inputs: 
------------------------------------------------
1. Input file name (containing the raw records) 
2. Ouput file name (containing sorted records) 
3. Main  memory size (in MB) 
4. Order code (asc/desc) asc means to sort in ascending order and desc means  to sort in descending order 
5. Columname1 
6. Columnname2
7. ..... 
