import time
import subprocess

if __name__ == "__main__":
    subprocess.Popen(["prefect", "orion" ,"start","--host", "127.0.0.1","--port", "20001"])
    while(True):
        print("Hello World")
        time.sleep(2)
    