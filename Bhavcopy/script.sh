#! /bin/sh
python3 /Users/ashutoshaggawal27/Documents/Term4/ProjectCourse/Term4Options/Bhavcopy/Bhavcopy.py 2021-09-03 2021-09-07
cat /Users/ashutoshaggawal27/Documents/Term4/ProjectCourse/Term4Options/dataset/Stocks/*.csv > /Users/ashutoshaggawal27/Documents/Term4/ProjectCourse/Term4Options/dataset/stocks.csv
cat /Users/ashutoshaggawal27/Documents/Term4/ProjectCourse/Term4Options/dataset/Futures/*.csv > /Users/ashutoshaggawal27/Documents/Term4/ProjectCourse/Term4Options/dataset/futures.csv
rm -r /Users/ashutoshaggawal27/Documents/Term4/ProjectCourse/Term4Options/dataset/Futures
rm -r /Users/ashutoshaggawal27/Documents/Term4/ProjectCourse/Term4Options/dataset/Stocks
python3 /Users/ashutoshaggawal27/Documents/Term4/ProjectCourse/Term4Options/Bhavcopy/data_processing.py 0.04