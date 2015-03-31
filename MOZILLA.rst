Dockerization
*************

Loads uses Docker containers to run services and test agents. Dockerization
requires creating a Docker image, which can be `automated
<http://docs.docker.com/reference/builder/>`_ via a ``Dockerfile``.

Loads can pull Docker images directly from the Docker Hub, or from a URL to
an image tarball. Because the images must be downloaded to every instance,
only small containers (10-15 MB) should be loaded from the Hub. Larger
containers should be uploaded to S3 for speed.

To create a tarball from a Docker image:

::

 > cd {project}
 > docker build -t={container_name} .
 > docker save -o={project}.tar {container_name}
 > bzip2 -z {project}.tar

Docker images for Mozilla services are stored in the ``loads-docker-images``
S3 bucket, under the ``cloudservices-aws-dev`` account. To upload Docker
tarballs to this bucket:

#. Sign in to the AWS Console, and choose "Services > Storage & Content
   Delivery > S3" from the drop-down at the top of the page.
#. Choose "loads-docker-images" from the bucket list.
#. Click the "Upload" button to open the file uploader, then add the
   ``{project}.tar.bz2`` file.
#. Click "Set Details >", then "Set Permissions >". You can skip the details
   screen; there's no need to enable redundant storage or encryption.
#. Click "Add more permissions" and select the "Everyone" grantee from the
   drop-down. Check the "Open/Download" box to allow public downloads.
#. Click "Start Upload".
#. Once the upload completes, the tarball will be accessible at
   ``https://s3.amazonaws.com/loads-docker-images/{project}.tar.bz2``. You
   can specify this URL in the project file's ``container_url`` field.
