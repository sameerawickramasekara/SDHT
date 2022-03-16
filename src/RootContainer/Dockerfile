FROM python:latest

ENV HOSTNETWORK="0.0.0.0"
ENV HOSTPORT=6423

# Put files at the image '/server/' folder.
ADD RootNode.py /server/
ADD constants.py /server/
# '/server/' is base directory
WORKDIR /server/
# Expose port 9898 in the container
EXPOSE 6000-7000
# execute the command
CMD python3 /server/RootNode.py ${HOSTNETWORK} ${HOSTPORT}