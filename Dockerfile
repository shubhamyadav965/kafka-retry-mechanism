# Use Node.js 22 as the base image.
# Your local Node version is 22.23.0.
FROM node:22-alpine

# Set the working directory inside the container.
WORKDIR /app

# Copy package files first.
# Docker can cache this layer, so if your source code changes
# but package.json does not, Docker doesn't need to reinstall
# all dependencies every time.
COPY package*.json ./

# Install dependencies.
RUN npm install

# Copy the rest of the NestJS project into the container.
COPY . .

# Build the NestJS application.
# TypeScript files are compiled into the dist/ directory.
RUN npm run build

# The NestJS application listens on port 3000.
EXPOSE 3000

# Start the compiled NestJS application.
CMD ["npm", "run", "start:prod"]