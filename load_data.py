# command run python load_data.py elasticsearch_url
import os
import sys
for file in os.listdir(str(sys.argv[1])):

print("Load data nyukin")
os.system("python load_nyukin ./nyukin " + str(sys.argv[1]))
print("Load data customer_pay")
os.system("python load_customer_pay ./customer_pay " + str(sys.argv[1]))
print("Load data seikyu")
os.system("python load_seikyu ./seikyu " + str(sys.argv[1]))
print("Load data azukari_history")
os.system("python load_azukari_history ./azukari_history " + str(sys.argv[1]))
