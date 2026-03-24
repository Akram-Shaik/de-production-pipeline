import pandas as pd
import sqlite3

def clean_name(full_name):
    # This function takes "John Albert Doe" and returns "John Doe"
    parts = str(full_name).split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[-1]}"
    return full_name

def get_standardized_data(filename):
    df = pd.read_csv(filename)
    df['name'] = df['name'].apply(clean_name).str.title().str.strip()
    df['department'] = df['department'].astype(str)
    df['salary'] = df['salary'].fillna(df['salary'].mean())
    return df

def run_my_first_pipeline():
    try:
        print("🚀 Pipeline Started...")

        # 1. EXTRACT: Read the CSV file
        df_employees = get_standardized_data('data.csv')
        print("✅ Data extracted from CSV.")

        # 2. TRANSFORM (CLEAN IMMEDIATELY)
        # Apply our custom clean_name function to every row
        """METHOD 1
        df_employees['name'] = df_employees['name'].apply(clean_name)
        df_employees['name'] = df_employees['name'].str.title().str.strip()

        # Fill missing salaries with the average salary so the math doesn't break
        df_employees['salary'] = df_employees['salary'].fillna(df_employees['salary'].mean())

        # 3.Filter (Using the now-clean data) and give them a 10% raise
        # This is a classic "Data Engineering" task
        eng_staff = df_employees[df_employees['department'] == 'Engineering'].copy()
        eng_staff['salary'] = eng_staff['salary'] * 1.20
        print("✅ Transformation complete: Engineering salaries increased by 20%.") """

        # METHOD 2
        # Filter for Engineering department first, then clean the names and handle missing salaries
        eng_staff = df_employees[df_employees['department'] == '1'].copy()
        eng_staff['salary'] = eng_staff['salary'] * 1.10


        # 4. LOAD: Save this into a SQL Database
        # This creates a file called 'company.db' automatically
        conn = sqlite3.connect('company.db')
        eng_staff.to_sql('engineering_promotions', conn, if_exists='replace', index=False)
        print("✅ Data loaded into SQL (Engineering_promotions table).")

        # 5. VERIFY: Query the SQL database to prove it worked
        print("\n--- Final SQL Report (High Earners) ---")
        report = pd.read_sql_query("SELECT name, salary FROM engineering_promotions WHERE salary > 80000", conn)
        print(report)

        # Strip extra spaces from names
        df_employees['name'] = df_employees['name'].str.strip().str.title() 
    
        
        #Second piece of code starts here group by()
        dept_summary = df_employees.groupby('department')['salary'].sum().reset_index()

        # 2. LOAD: Save this summary into a NEW SQL table
        dept_summary.to_sql('department_stats', conn, if_exists='replace', index=False)

        print("✅ Department summary created and loaded to SQL.")

        # 3. VERIFY: Query the new table
        print("\n--- Department Salary Spend ---")
        stats_report = pd.read_sql_query("SELECT * FROM department_stats", conn)
        print(stats_report)
    except Exception as e:
        # --- THE SAFETY NET ---
        print(f"❌ STOP! Something went wrong: {e}")
        print("💡 Hint: Check if your 'data.csv' is missing a column or has a typo.")

        conn.close()
    print("\n🏆 Pipeline finished successfully!")

def run_second_pipeline():
    
    try:
        print("\n\n=======Second_pipeline=========")
        print("🚀 Pipeline Started...")
        #second DataFrame
        # 1. EXTRACT: Read the CSV file again for the second DataFrame
        df_employees = get_standardized_data('data.csv')
        df_dept = pd.read_csv('departments.csv')
        print("✅ Data extracted from departments.csv.")

        #this step we added after the merging opeartion is done.
        #After merging, we got an error:int64 and object data types. We need to convert the dept_id to string before merging
        df_employees['department'] = df_employees['department'].astype(str)
        df_dept['dept_id'] = df_dept['dept_id'].astype(str)

        # LOAD: Save this into a SQL Database
        # This creates a file called 'company.db' automatically
        conn = sqlite3.connect('company.db')
        df_dept.to_sql('manager_data', conn, if_exists='replace', index=False)
        print("✅ Data loaded into SQL (manager_data table).")
        
        #Merge the two tables to get a combined view of employees and their managers
        df_final = pd.merge(df_employees, df_dept, left_on='department', right_on='dept_id', how='left')

        #Save the MERGED result into a new SQL table
        df_final.to_sql('employee_manager_data', conn, if_exists='replace', index=False)
        print("✅ Merged employee and manager data loaded into SQL (employee_manager_data table).")

        #cross checking the table
        final_table=pd.read_sql_query("select * from employee_manager_data", conn)
        print("\n--- Final Merged Data ---")
        print(final_table)

    except Exception as e:
        print(f"❌ STOP! Something went wrong while reading departments.csv: {e}")
        return
    
    conn.close()
    print("\n🏆 Pipeline finished successfully!")

if __name__ == "__main__":
    run_my_first_pipeline()
    run_second_pipeline()