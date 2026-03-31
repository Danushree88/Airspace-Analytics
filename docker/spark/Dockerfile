FROM spark:3.5.7

USER root

# Install Python dependencies permanently
RUN pip install --no-cache-dir \
    scikit-learn \
    pandas \
    numpy \
    scipy \
    joblib

USER spark