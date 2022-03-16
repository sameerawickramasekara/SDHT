FROM python:latest

ENV ROOTHOST="0.0.0.0"
ENV ROOTPORT=6423

ENV PORT=6424
ENV HOSTNETWORK=sdht_root
# Put files at the image '/server/' folder.
ADD Chord.py /client/
ADD constants.py /client/
ADD NeighbourNode.py /client/
ADD NodeState.py /client/
ADD PeriodicWorker.py /client/
ADD NodeWorker.py /client/
ADD utils.py /client/
# '/server/' is base directory
WORKDIR /client/
# Expose port 9898 in the container
EXPOSE 6000-7000
# execute the command
CMD python3 /client/Chord.py ${ROOTHOST} ${ROOTPORT} ${PORT} ${HOSTNETWORK}
# CMD [ "python3", "/client/app.py" ]