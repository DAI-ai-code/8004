FROM ubuntu
RUN apt update -y
RUN apt install python3 -y
RUN apt install python3-pandas -y
RUN mkdir /app
COPY cal_house.py /app/
COPY housing.csv /app/
CMD [ "/bin/python3", "app/cal_house.py" ]
