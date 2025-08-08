Steps

1. download docker desktop
2. https://docs.docker.com/desktop/setup/install/windows-install/
2. https://git-scm.com/download/win
3. git clone https://github.com/chrislan815/mlbet/tree/main
run this first

```shell
docker build -t mlbet .
```

then this 

```shell
docker run -it \
  -v /Users/chris.lan/Downloads/mlb.db:/mlb.db \
  -v /Users/chris.lan/clan/mlbet/games:/games \
  -v /Users/chris.lan/clan/mlbet/live_feeds:/live_feeds \
  mlbet \
  python pull_games.py \
    --db /mlb.db \
    --start-date 2025-08-05 \
    --skip-weather
```

here's window version. it's oneliner to avoid issues with line continuation in cmd.exe


```shell
docker run -it -v "C:\Users\chris.lan\Downloads\mlb.db:/mlb.db" -v "C:\Users\chris.lan\clan\mlbet\games:/games" -v "C:\Users\chris.lan\clan\mlbet\live_feeds:/live_feeds" mlbet python pull_games.py --db /mlb.db --start-date 2025-08-05 --skip-weather
```

If it still says “unable to open database file,” it’s almost always the Windows → Docker file sharing permissions.