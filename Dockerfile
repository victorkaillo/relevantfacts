FROM python:3.8
RUN echo "deb http://deb.debian.org/debian/ unstable main contrib non-free" >> /etc/apt/sources.list.d/debian.list

# UPDATE APT-GET
RUN apt-get -y update
# Adding Firefox to the repositores
# Install Firefox
RUN apt-get install -y --no-install-recommends firefox
# Download GeckoDriver [ FIREFOX ]
RUN wget https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz
# Unzip GeckoDriver
RUN tar -xvzf geckodriver*
# Make executable
RUN chmod +x geckodriver
# Move to PATH
RUN mv geckodriver /usr/local/bin/
# Delete tar.gz GeckoDriver
RUN rm geckodriver-v0.30.0-linux64.tar.gz

# PYODBC DEPENDENCES
RUN apt-get install -y tdsodbc unixodbc-dev
RUN apt install unixodbc -y
RUN apt-get clean -y

# UPGRADE pip3
RUN pip3 install --upgrade pip

# DEPENDECES FOR DOWNLOAD ODBC DRIVER
RUN apt-get install apt-transport-https 
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update

# INSTALL ODBC DRIVER
RUN ACCEPT_EULA=Y apt-get install msodbcsql17 --assume-yes


# RUN apt-get update && apt-get install -y unixodbc unixodbc-dev
# COPY ./dremio-odbc-1.5.1.1001-1.x86_64.rpm .
# RUN yes|apt-get install alien 
# RUN alien -i ./dremio-odbc-1.5.1.1001-1.x86_64.rpm

COPY . /app
COPY './requirements.txt' .
COPY './.env' .

WORKDIR /app
RUN apt-get update && apt-get install -y git
RUN pip install --no-cache-dir fastapi
RUN pip install --no-cache-dir -r requirements.txt 
# RUN pip uninstall h11
RUN pip install h11


ENV PORT=5018
EXPOSE $PORT


# Set display port as an environment variable
ENV DISPLAY=:99
CMD ["python", "run.py"]
# CMD gunicorn --log-file=- main:app --bind 0.0.0.0:$PORT --timeout 2200 -w 4 -k uvicorn.workers.UvicornWorker