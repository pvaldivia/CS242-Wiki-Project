from fsplit.filesplit import FileSplit

fs = FileSplit(file='./wiki.json', splitsize=10240000, output_dir='../data')
fs.split()