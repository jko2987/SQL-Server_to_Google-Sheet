*************************************************************************************************************************
HOW TO INSTALL:

	1 - In the directory \***relative_path***\SQL_to_GSheet\dist\
	2 - Run the sql_to_gsheet.x.x-amd64.msi file
	3 - Select the directory to save the program to
	4 - Click Next
	5 - Click Finish when complete

	***********************************************************
	SETUP

	6 - ADDITIONAL REQUIREMENT: MICROSOFT ODBC DRIVER 17 FOR SQL SERVER Needs to be installed on the host computer

	    	LINK: https://www.microsoft.com/en-us/download/details.aspx?id=56567

	
	7 - You need a service account file that has access to edit the destination sheet. Name it gsheet_creds.json

*************************************************************************************************************************
HOW TO USE:
	1 - Create a SQL folder which will hold your SQL to GSheet folders
	2 - In your SQL to GSheet Settings folder create a folder for your query
	3 - In this folder you will create 2 files
		- gsheet_info.txt (BQ Settings file: DO NOT RENAME THIS FILE)

			 ################################################

			     GSheet_ID
        		 Name of Destination Sheet
        		 Cell to past dataframe in

			 ################################################

		- db_shortname-query_name.txt (This contains your query
			db_shortname-query.txt
			# the first portion of this filename (before the hyphen) is a database shortname that is hardcoded 
			  in the python code itself which will set the server and database
			# the second portion of this filename
    4 - Share your GSheet/protected range with the service account email
	5 - Create a folder for your .bat files
	6 - In that folder create a .bat file that contains the format below

			#######################################################################
		    cd "Path\to\SQL_to_GSheet\1build\"
			start sql_to_gsheet.exe "---PATH TO YOUR QUERY FOLDER---"

			#######################################################################

	7 - Run the file and you are finished


	COMMON Issues/Nuances
	    - No data pasting in GSheet - make sure you shared your GSheet and protected range with gsheet service account
	    - Data in GSheet doesn't look how I want it - All formatting of the data should be done in your SQL Query
		

*************************************************************************************************************************