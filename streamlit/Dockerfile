FROM continuumio/miniconda3

WORKDIR /home/app

COPY requirements.txt /home/app
RUN pip install --no-cache-dir -r requirements.txt

CMD streamlit run --server.port $PORT streamlit_app.py