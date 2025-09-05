FROM mcr.microsoft.com/playwright:v1.48.2-focal

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY bridge.js ./

EXPOSE 9127

CMD ["node", "bridge.js"]
