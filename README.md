# pie_mail
My first project used to send mails using Python.

This code is a basic mail sending code with Python, found on the web.

It has been customised so as to cater to my needs.

This modified version of code, aims at sending an Email with a table as an inline attachment an also a CSV file as an actual attachment.

The code facilitates running on a serverless framework like AWS Lambda e.t.c.

Now, in reference to the attachments in the Email:

1. Inline Table.
    An SQL Connection is initiated from the code itself and the resultant output is transformed into a pandas dataframe.
    The pandas dataframe in turn is styled with the help of HTML to apply desired styling(colors, fonts and grids) to the table.    
  
2. CSV file attachment.
    The secondary aim of our project is to add an `CSV` file as an attachment.
    The CSV file is created in the code itself, an SQL Query is run and the output is written into a CSV file, in turn the file is           attached as a payload to the mail.
    

Some Issues and Workarounds:
● Applying styling to the dataframe requires the indices to be unique.
  Using groupby() may help in creating indices with unique values.
● Even after applying the styling to the dataframes, some mail clients do not support a few
  kinds of styling tags.
  premailer is used so that the HTML styling of the tables is not lost while using different
  mail clients to view the email and enable cross compatibility.
  Ex: GMail doesn’t support HTML styling tags without inline CSS.
  Hence, premailer is used to convert the code-generated HTML string to GMail compatible
  HTML scripts.
● Be mindful of the styling being applied to the dataframe too.
  As the more styling attributes we provide, the more the size of the HTML string increases
  and GMail supports only 120 Kb of inline data , which may lead to a clipping of the
  message.
● If the code is being run on a serverless machine(like AWS Lambda) for any attachments
  created, the file path needs to be modified with path= r'/tmp/ ' , so as to reference the
  temporary storage.
