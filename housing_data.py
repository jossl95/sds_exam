##############################
#                            #
#    Housing Data Scraper    #
#                            #
##############################

max_page = 288
for page in range(max_page+1):
    url = f'https://www.boliga.dk/salg/resultater?salesDateMin=1995&zipcodeFrom=1000&zipcodeTo=2499&searchTab=1&page={page}&sort=date-a'
    print(url)
