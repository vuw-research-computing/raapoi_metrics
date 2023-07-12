import os

def gen_webpage():
    output_file = "index.html"
    img_dir = "/var/www/html/plots"  # replace this with the path to your images

    sections = ['monthly_costs', 'monthly_users', 'yearly_costs', 'yearly_users']

    html = """
    <!doctype html>
    <html lang="en">
    <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" rel="stylesheet">
    <title>Raapoi Cluster Reports</title>
    </head>
    <body>
    <div class="container">
        <h1>Raapoi Cluster Status and Docs </h1>
        <p>
            <A HREF=http://raapoi.vuw.ac.nz/grafana-slurm/public-dashboards/b2e6bd9703b847ccb6edca9f2deb2a43?orgId=1&refresh=30s>General Cluster Status 24h</A>
        </p>
        <p>    
        <A HREF=https://vuw-research-computing.github.io/raapoi-docs/>Raapoi Cluster Documentation</A>
        </p>
        <p>
            <A HREF=http://raapoi.vuw.ac.nz//grafana-slurm>Graphana Monitoring Tool - admin login</A>
        </p> 
        </div>
    </div>
    <div class="container">
        <h1>Raapoi Cluster Reports and Usage</h1>
    """

    for section in sections:
        html += f"<h2>{section.capitalize()}</h2>"
        html += '<div class="row">'
        for file in os.listdir(os.path.join(img_dir, section)):
            if file.endswith(".png"):
                html += f"""
                <div class="col-sm-4">
                <div class="card">
                    <img class="card-img-top" src="/plots/{section}/{file}" alt="{file}">
                    <div class="card-body">
                    <p class="card-text">{file}</p>
                    </div>
                </div>
                </div>
                """
        html += '</div>'

    html += """
    </div>
    <script src="https://code.jquery.com/jquery-3.3.1.slim.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.14.7/umd/popper.min.js"></script>
    <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/js/bootstrap.min.js"></script>
    </body>
    </html>
    """

    with open(output_file, 'w') as f:
        f.write(html)
