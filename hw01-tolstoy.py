#Importing library to read the html link
import urllib.request
file = urllib.request.urlopen('https://www.gutenberg.org/files/2600/2600-h/2600-h.htm')

#Reading the data in the html link
data=file.read()

#Declaring list to have unique words
unique = []

#Punctuation string including all the punctuations
punctuation1 = '''!@#$%^&*()_+-=:;",.<>|?/'''

#Declaring no punction list to have words without punctuation
nopunc = []

split_data = data.split()

#Removing punctuations in the words
for i in split_data:
    n = i.decode('UTF-8')
    for j in range(0,len(n)):
        if n[j] in punctuation1:
            n = n.replace(n[j]," ")
    #Converting all the words to lower case
    n = n.lower()
    nopunc.append(n)
#Removing spaces in single word
nospace = [p.replace(" ", "") for p in nopunc]

#Appending unique list with unique words
for k in nospace:
    if k not in unique:
        unique.append(k)

#Printing total number of unique words
print('Total number of unique words in the HTML file',len(unique))
