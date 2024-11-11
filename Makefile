build_dockerimage:
	#docker buildx build --no-cache -f Dockerfile -t spark3io-sbt:latest .
	docker buildx build -f Dockerfile -t spark3io-sbt:latest .