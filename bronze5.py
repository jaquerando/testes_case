def fetch_and_check_breweries():
    log_messages = []
    log_messages.append(f"Execução da DAG: {datetime.utcnow().isoformat()}")

    try:
        url = "https://api.openbrewerydb.org/breweries"
        response = requests.head(url)
        response.raise_for_status()
        
        last_modified = response.headers.get("Last-Modified")
        if last_modified:
            last_modified_date = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")
            log_messages.append(f"Última modificação no endpoint: {last_modified}")
        else:
            log_messages.append("Cabeçalho Last-Modified não encontrado. Atualização forçada.")
            last_modified_date = datetime.utcnow()

        client = storage.Client()
        bucket = client.get_bucket('bucket-case-abinbev')
        blob = bucket.blob('data/bronze/breweries_raw.json')

        # Verifique se o blob possui metadados antes de tentar acessar `get("last_update")`
        if blob.exists() and blob.metadata:
            gcs_last_update = blob.metadata.get("last_update")
            if gcs_last_update:
                gcs_last_update_date = datetime.strptime(gcs_last_update, "%Y-%m-%dT%H:%M:%SZ")
                if last_modified_date <= gcs_last_update_date:
                    log_messages.append("Nenhuma atualização detectada no endpoint. Dados não atualizados no bucket.")
                    save_log(log_messages)
                    return

        log_messages.append("Atualização detectada. Iniciando o download.")
        response = requests.get(url)
        response.raise_for_status()
        breweries = response.json()
        json_lines = "\n".join([json.dumps(brewery) for brewery in breweries])

        blob.upload_from_string(json_lines, content_type='application/json')
        blob.metadata = {"last_update": last_modified_date.strftime("%Y-%m-%dT%H:%M:%SZ")}
        blob.patch()
        log_messages.append("Arquivo atualizado com sucesso no bucket GCS.")

    except requests.exceptions.RequestException as e:
        log_messages.append(f"Erro ao acessar o endpoint: {e}")
        logging.error(f"Erro ao acessar o endpoint: {e}")
        raise

    except Exception as e:
        log_messages.append(f"Erro ao salvar no bucket GCS: {e}")
        logging.error(f"Erro ao salvar no bucket GCS: {e}")
        raise

    save_log(log_messages)
