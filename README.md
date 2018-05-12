# home-automation
My home automation stuff

## How to set up the environment

You'll need an MQTT broker

    docker run -d --name eclipse-mosquitto --restart=always -p 1883:1883 -p 9001:9001 eclipse-mosquitto

And you'll need the Virtualenv

    mkvirtualenv -p `which python3` home-automation
    pip install -r requirements.txt
