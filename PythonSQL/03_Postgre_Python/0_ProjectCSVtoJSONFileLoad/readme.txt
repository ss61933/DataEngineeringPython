#PROJECT SCOPE

-READ FILES  IN A LOOP FROM RETAIL_DB FOLDER WHICH HAVE SUB FOLDER for 
    CATEGORIES | CUSTOMERS | DEPARTMENTS | ORDER_ITEMS | ORDERS | PRODUCTS 
-READ INDIVIDUAL FILES COLUMN STRCTURE FROM SCHEMAS.JSON
-READ THE FILES USING PANDAS AND CONVERT IT TO JSON
-WRITE THE FILES INTO TARGET RETAIL_DB_JSON FOLDER
-IN CASE WRONG FILE NAME IS PROVIDED THROW THE ERROR MESSAGE AND CONTINUE WITH OTHER FILES


1. Setup enviornment
    1. Create new virutal enviornment
    2. Make sure project is setup using virtual enviornment  by selecting interpreter
    3. Open the terminal and verify the same
    4. Install all dependencies using requirments.txt using terminal
       pip install -r requirments.txt
    5. Create app.py file which will have all code 
        Modularize the code using functions
    6. Setup the source and target dir variables to read from enviornment variables
       In real time ETL tool make sure to pass this values    
        We need to set the env variable for every new session
        In windows use
        $Env:TRG_BASE_DIR = "F:/GitHub/PythonDataEngineering/PythonSQL/03_Postgre_Python/data-master/retail_db_JSON1"
        In Mac / Linux 
        Export TRG_BASE_DIR= " "
    7. Pass JSON array to read file structure as well as a env variable
        For this we take JSON array , convert it to List and pass as arg
        Invoke using , dept is wrong value to test the failure scenario
        python app.py '[\"orders\","\dept\",\"order_items\"]'
        pyhon app.py # To test if all files processed 
    8.  Handle excption for below scenario
        If wrong file name is passed as a argument then throw warning and continue 
    9. install pip install python-dotenv to use .env

    Run the program using :python app.py '[\"dept\",\"orders\"]'

    Expected output 
    ----processing file dept
No files found for dept
-----Error processing dept
Error processing dept
----processing file orders
Populating chunk 0 of orders
Populating chunk 1 of orders
Populating chunk 2 of orders
Populating chunk 3 of orders
Populating chunk 4 of orders
Populating chunk 5 of orders
Populating chunk 6 of orders

    