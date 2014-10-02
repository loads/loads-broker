############################################################
# Dockerfile to run Loads Broker Containers
# Based on Debian Image
############################################################

# Set the base image to use to Debian
FROM ubuntu

MAINTAINER Mozilla Cloud Services

RUN apt-get update
RUN apt-get install -y python3-pip
RUN apt-get install -y python-virtualenv
RUN apt-get install -y git
RUN apt-get -y install openssh-server
RUN mkdir /var/run/sshd
RUN sed -i "s/#PasswordAuthentication yes/PasswordAuthentication no/" /etc/ssh/sshd_config
RUN sed -i s#/home/git:/bin/false#/home/git:/bin/bash# /etc/passwd

# Adding loads user
RUN adduser --system loads
RUN mkdir -p /home/loads/.ssh

# Clearing and setting authorized ssh keys
RUN echo '' > /home/loads/.ssh/authorized_keys
RUN echo 'ssh-dss AAAAB3NzaC1kc3MAAACBAO1ruCNM0UDWugGQtsmO3B+LGJ+LxXVmQWo+53fOD+m8vQy98wFVpTTUWFXTXKUpAUkhgUOTuabIYSkEnpiDF9MasPFFwVHh9SNq1lWLifmPrHVGZ3+P5t6zskD51HaxeOuJJDasTIMxq8+d0TUP6SoGIzOWWU/GTVooqXsZ2/WnAAAAFQDdWZgnOsQm7FQWWYMSsP7LXXNrPwAAAIBRDreK4rnfeZU4oeLrnvMlakdzjAgCso7utwgZva95i7qjDHQODpWos+hjjLf6naRraXGnd2FVkep10luWzKiQpC4Hdy5s91203ZBZy/fw8coubNgt1Smd+Zi89yTLEe+xUYrSVoxfARY/e0DdRJmBb1ifkooAyujdTEhXmUDNLAAAAIBNUiOOL0s0m5Nth+fihLX7vwcexMCFS4PfnoERlAxj2tCWiNl8IEFbfYyiNMGVk9pjTnABpyaVCcNS8KopnPosvYwnSgpPUvZn7ssRacXwMF7PYd8h7iDN33jcmxGPLfnLwz+f6ODwvjIQOzAsPeNPh7d15NOVI6p/3O8LlgV+Jw== tarek@foobook.local' >> /home/loads/.ssh/authorized_keys

RUN git clone https://github.com/loads/loads-broker
RUN cd loads-broker; make build

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
