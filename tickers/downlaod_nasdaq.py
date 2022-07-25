import ftplib
import os
import re
 
os.chdir("./tickers")
# Connect to ftp.nasdaqtrader.com
ftp = ftplib.FTP('ftp.nasdaqtrader.com', 'anonymous', 'anonymous@debian.org')
 
# Download files nasdaqlisted.txt and otherlisted.txt from ftp.nasdaqtrader.com
for ficheiro in ["nasdaqlisted.txt", "otherlisted.txt"]:
        ftp.cwd("/SymbolDirectory")
        localfile = open(ficheiro, 'wb')
        ftp.retrbinary('RETR ' + ficheiro, localfile.write)
        localfile.close()
ftp.quit()
 
# Grep for common stock in nasdaqlisted.txt and otherlisted.txt
for ficheiro in ["nasdaqlisted.txt", "otherlisted.txt"]:
        localfile = open(ficheiro, 'r')
        for line in localfile:
                if re.search("Common Stock", line):
                        ticker = line.split("|")[0]
                        # Append tickers to file tickers.csv
                        open("nasdaq.csv","a+").write(ticker + "\n")