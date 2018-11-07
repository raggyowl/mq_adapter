FROM scratch
EXPOSE 9090
COPY . /
CMD ["/build"]