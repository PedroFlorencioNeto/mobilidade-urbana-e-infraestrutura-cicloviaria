import prefect

def log(msg):
    prefect.context.logger.info(f'\n{msg}')