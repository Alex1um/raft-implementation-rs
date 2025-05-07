# Import necessary libraries
from flask import Flask, jsonify, render_template_string
import docker
import sys

# Initialize the Flask application
app = Flask(__name__)

# HTML template for displaying the table
# Using Tailwind CSS classes for basic styling (assuming Tailwind is available or added)
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Docker Containers</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        /* Optional: Add custom styles if needed */
        body {
            font-family: 'Inter', sans-serif;
            background-color: #f3f4f6; /* Light grey background */
            display: flex;
            justify-content: center;
            align-items: flex-start; /* Align items to the top */
            min-height: 100vh; /* Minimum height of viewport */
            padding: 20px; /* Add some padding around the content */
        }
        .container-table {
            background-color: #ffffff; /* White background for the table */
            border-radius: 0.5rem; /* Rounded corners */
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); /* Subtle shadow */
            overflow: hidden; /* Hide overflowing content */
            max-width: 800px; /* Max width for the table */
            width: 100%; /* Full width up to max-width */
        }
        .container-table > table {
            width: 100%; /* Full width up to max-width */
        }
        container-table {
            background-color: #ffffff; /* White background for the table */
            border-radius: 0.5rem; /* Rounded corners */
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); /* Subtle shadow */
            overflow: hidden; /* Hide overflowing content */
            max-width: 800px; /* Max width for the table */
            width: 100%; /* Full width up to max-width */
        }
        th, td {
            padding: 12px 15px; /* Padding for table cells */
            text-align: left; /* Align text to the left */
        }
        th {
            background-color: #4f46e5; /* Indigo background for header */
            color: white; /* White text for header */
            font-weight: bold; /* Bold font for header */
        }
        tr:nth-child(even) {
            background-color: #eef2ff; /* Light indigo background for even rows */
        }
        tr:hover {
            background-color: #d1d5db; /* Light grey background on hover */
            cursor: pointer;
        }
        caption {
            caption-side: top;
            text-align: left;
            font-size: 1.25rem; /* Larger font size for caption */
            font-weight: bold;
            padding: 15px;
            color: #374151; /* Dark grey color for caption */
        }
    </style>
</head>
<body>
    <div class="container-table">
        <table>
            <caption>Docker Container IP Addresses</caption>
            <thead>
                <tr>
                    <th>Container Name</th>
                    <th>IP Address</th>
                    <th>Stop</th>
                </tr>
            </thead>
            <tbody>
                {% for container in containers %}
                <tr onclick="window.location='http://{{ container.ip }}:8000'">
                    <td>{{ container.name }}</td>
                    <td>{{ container.ip }}</td>
                    <td><a href="http://{{ container.ip }}:8000/stop">stop</a></td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</body>
</html>
"""

# Route to get container information and display as HTML
@app.route('/')
@app.route('/containers')
def list_containers():
    """
    Fetches Docker container names and IP addresses and renders them in an HTML table.
    """
    container_list = []
    try:
        # Initialize the Docker client
        client = docker.from_env()

        # List all running containers
        containers = client.containers.list()

        # Iterate through containers to get name and IP
        for container in containers:
            name = container.name
            ip_address = "N/A" # Default value if IP is not found

            # Docker networking can be complex. This attempts to find an IP
            # in the available networks. You might need to adjust this
            # based on your specific network setup.
            if container.attrs['NetworkSettings']['Networks']:
                # Get the first network the container is connected to
                network_name = list(container.attrs['NetworkSettings']['Networks'].keys())[0]
                ip_address = container.attrs['NetworkSettings']['Networks'][network_name].get('IPAddress', 'N/A')

            container_list.append({'name': name, 'ip': ip_address})

    except docker.errors.DockerException as e:
        # Handle Docker errors (e.g., Docker not running, permission issues)
        return f"Error connecting to Docker: {e}", 500
    except Exception as e:
        # Handle other potential errors
        return f"An unexpected error occurred: {e}", 500

    # Render the HTML template with container data
    return render_template_string(HTML_TEMPLATE, containers=container_list)

# Route to get container information as JSON
@app.route('/api/containers')
def list_containers_json():
    """
    Fetches Docker container names and IP addresses and returns them as JSON.
    """
    container_list = []
    try:
        client = docker.from_env()
        containers = client.containers.list()

        for container in containers:
            name = container.name
            ip_address = "N/A"

            if container.attrs['NetworkSettings']['Networks']:
                network_name = list(container.attrs['NetworkSettings']['Networks'].keys())[0]
                ip_address = container.attrs['NetworkSettings']['Networks'][network_name].get('IPAddress', 'N/A')

            container_list.append({'name': name, 'ip': ip_address})

    except docker.errors.DockerException as e:
        return jsonify({"error": f"Error connecting to Docker: {e}"}), 500
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 500

    return jsonify(container_list)


# Run the Flask app
if __name__ == '__main__':
    # Ensure the script is run with sufficient permissions to access the Docker socket
    if sys.platform.startswith('linux') or sys.platform == 'darwin':
        # On Linux/macOS, check if running as root or user in docker group
        try:
            import os
            if os.geteuid() != 0:
                 # Check if user is in 'docker' group
                 import subprocess
                 try:
                     groups = subprocess.check_output(['id', '-Gn']).decode().split()
                     if 'docker' not in groups:
                         print("Warning: Not running as root or in the 'docker' group. May not be able to access Docker socket.")
                         print("Try running with 'sudo python your_script_name.py' or add your user to the 'docker' group.")
                 except Exception:
                     print("Warning: Could not verify user group membership. Ensure you have permissions to access the Docker socket.")
        except ImportError:
             print("Warning: Could not check user permissions. Ensure you have permissions to access the Docker socket.")
    elif sys.platform == 'win32':
        # On Windows, Docker Desktop usually handles permissions
        pass
    else:
        print(f"Warning: Running on an unsupported platform ({sys.platform}). Ensure you have permissions to access the Docker socket.")


    # The app will be accessible at http://127.0.0.1:5000/ or http://<your-ip>:5000/
    # debug=True allows for automatic code reloading and better error messages during development
    app.run(debug=True, host='0.0.0.0') # Listen on all available interfaces
