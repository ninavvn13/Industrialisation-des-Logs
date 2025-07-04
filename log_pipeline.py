# log_pipeline.py
import json
import time
import re
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging

# Importer la classe LogParser depuis log_parser.py
try:
    from log_parser import LogParser
except ImportError:
    print("Erreur: Impossible d'importer LogParser. Assurez-vous que log_parser.py est dans le même répertoire.")
    exit(1)

class LogPipeline:
    def __init__(self, log_file_path: str = 'app.log'):
        self.log_file_path = log_file_path
        self.parser = LogParser() # Utilise le parser de logs existant
        self.processed_logs_count = 0
        self.start_time = None
        self.end_time = None

        # Agrégations en temps réel
        self.realtime_aggregations = {
            'event_type_counts': {},
            'hourly_traffic': {hour: 0 for hour in range(24)},
            'location_traffic': {},
            'device_type_traffic': {},
            'os_traffic': {},
            'purchase_summary': {'total_amount': 0.0, 'count': 0},
            'error_counts': {},
            'session_duration_sum': 0,
            'session_duration_count': 0
        }

        # Configuration du logger pour le pipeline
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pipeline.log'), # Logs spécifiques au pipeline
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _parse_user_agent(self, user_agent: str) -> Dict[str, str]:
        """
        Parse le user agent pour extraire le type d'appareil et le système d'exploitation.
        """
        device_type = 'Unknown'
        os_name = 'Unknown'

        # Détection du type d'appareil
        if 'Mobile' in user_agent or 'Android' in user_agent or 'iPhone' in user_agent or 'iPad' in user_agent:
            if 'iPad' in user_agent:
                device_type = 'Tablet'
            else:
                device_type = 'Mobile'
        elif 'Windows' in user_agent or 'Macintosh' in user_agent or 'X11' in user_agent or 'Linux' in user_agent:
            device_type = 'Desktop'

        # Détection du système d'exploitation
        if 'Windows NT 10.0' in user_agent:
            os_name = 'Windows 10'
        elif 'Windows NT' in user_agent:
            os_name = 'Windows (Older)'
        elif 'Macintosh; Intel Mac OS X' in user_agent:
            os_name = 'macOS'
        elif 'Android' in user_agent:
            os_name = 'Android'
        elif 'iPhone' in user_agent or 'iPad' in user_agent:
            os_name = 'iOS'
        elif 'Ubuntu' in user_agent or 'Linux' in user_agent:
            os_name = 'Linux'

        return {'device_type': device_type, 'os_name': os_name}

    def _enrich_log_entry(self, entry: Dict) -> Dict:
        """
        Enrichit une entrée de log parsée avec des informations supplémentaires.
        """
        # Enrichissement avec le type d'appareil et le système d'exploitation
        user_agent_info = self._parse_user_agent(entry.get('user_agent', ''))
        entry['device_type'] = user_agent_info['device_type']
        entry['os_name'] = user_agent_info['os_name']

        # La géolocalisation (location) est déjà présente dans les logs générés par app.py
        # On peut la récupérer directement si elle existe
        entry['location'] = entry.get('location', 'Unknown')

        return entry

    def _update_aggregations(self, entry: Dict):
        """
        Met à jour les agrégations en temps réel avec les données de l'entrée de log.
        """
        event_type = entry.get('event_type')
        timestamp_str = entry.get('timestamp')
        location = entry.get('location')
        device_type = entry.get('device_type')
        os_name = entry.get('os_name')

        # Compteur d'événements par type
        self.realtime_aggregations['event_type_counts'][event_type] = \
            self.realtime_aggregations['event_type_counts'].get(event_type, 0) + 1

        # Trafic horaire
        try:
            event_hour = datetime.fromisoformat(timestamp_str).hour
            self.realtime_aggregations['hourly_traffic'][event_hour] = \
                self.realtime_aggregations['hourly_traffic'].get(event_hour, 0) + 1
        except (ValueError, TypeError):
            self.logger.warning(f"Could not parse timestamp for hourly traffic: {timestamp_str}")

        # Trafic par localisation
        self.realtime_aggregations['location_traffic'][location] = \
            self.realtime_aggregations['location_traffic'].get(location, 0) + 1

        # Trafic par type d'appareil
        self.realtime_aggregations['device_type_traffic'][device_type] = \
            self.realtime_aggregations['device_type_traffic'].get(device_type, 0) + 1

        # Trafic par OS
        self.realtime_aggregations['os_traffic'][os_name] = \
            self.realtime_aggregations['os_traffic'].get(os_name, 0) + 1

        # Agrégations spécifiques pour les achats
        if event_type == 'purchase':
            total_amount = entry['data'].get('total_amount', 0.0)
            self.realtime_aggregations['purchase_summary']['total_amount'] += total_amount
            self.realtime_aggregations['purchase_summary']['count'] += 1

        # Agrégations pour les erreurs
        if event_type == 'error':
            error_code = entry['data'].get('error_code', 'UNKNOWN_ERROR')
            self.realtime_aggregations['error_counts'][error_code] = \
                self.realtime_aggregations['error_counts'].get(error_code, 0) + 1

        # Agrégations pour la durée de session
        if event_type == 'user_session_end':
            duration = entry['data'].get('duration_seconds', 0)
            self.realtime_aggregations['session_duration_sum'] += duration
            self.realtime_aggregations['session_duration_count'] += 1


    def process_logs_streaming(self, interval_seconds: float = 1.0):
        """
        Lit le fichier de log en streaming (simulé par une lecture ligne par ligne
        avec un délai) et traite chaque entrée.
        """
        self.logger.info(f"Démarrage du pipeline de traitement des logs en streaming depuis '{self.log_file_path}'...")
        self.start_time = datetime.now()

        # Vérifier si le fichier existe, sinon attendre qu'il soit créé par app.py
        while not os.path.exists(self.log_file_path):
            self.logger.info(f"En attente de la création du fichier de log '{self.log_file_path}'...")
            time.sleep(5) # Attendre 5 secondes avant de vérifier à nouveau

        # Ouvrir le fichier en mode lecture (tail -f like behavior)
        with open(self.log_file_path, 'r') as f:
            # Aller à la fin du fichier pour ne lire que les nouvelles lignes
            f.seek(0, os.SEEK_END)
            self.logger.info("Fichier de log ouvert. En attente de nouvelles lignes...")

            while True:
                line = f.readline()
                if not line:
                    # Pas de nouvelle ligne, attendre un peu avant de réessayer
                    time.sleep(interval_seconds)
                    continue

                self.processed_logs_count += 1
                parsed_log = self.parser.parse_log_line(line)

                if parsed_log:
                    enriched_log = self._enrich_log_entry(parsed_log)
                    self._update_aggregations(enriched_log)
                    # self.logger.debug(f"Processed: {enriched_log['event_type']} - Location: {enriched_log['location']} - Device: {enriched_log['device_type']}")
                else:
                    self.logger.warning(f"Skipping malformed or invalid log: {line.strip()}")

                # Afficher les statistiques de performance toutes les 100 logs traités
                if self.processed_logs_count % 100 == 0:
                    self.display_performance_metrics()
                    self.display_realtime_aggregations()


    def display_performance_metrics(self):
        """Affiche les métriques de performance du pipeline."""
        if self.start_time:
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            logs_per_second = self.processed_logs_count / elapsed_time if elapsed_time > 0 else 0

            self.logger.info("\n--- Métriques de Performance ---")
            self.logger.info(f"Logs traités au total: {self.processed_logs_count}")
            self.logger.info(f"Logs parsés avec succès: {self.parser.get_stats()['parsed']}")
            self.logger.info(f"Logs échoués au parsing/validation: {self.parser.get_stats()['failed']}")
            self.logger.info(f"Temps écoulé: {elapsed_time:.2f} secondes")
            self.logger.info(f"Logs par seconde: {logs_per_second:.2f}")
            self.logger.info("--------------------------------\n")

    def display_realtime_aggregations(self):
        """Affiche les agrégations en temps réel."""
        self.logger.info("\n--- Agrégations en Temps Réel ---")
        self.logger.info("Compteurs d'événements par type:")
        for event_type, count in self.realtime_aggregations['event_type_counts'].items():
            self.logger.info(f"  - {event_type}: {count}")

        self.logger.info("\nTrafic horaire (par heure de la journée):")
        sorted_hourly_traffic = sorted(self.realtime_aggregations['hourly_traffic'].items())
        for hour, count in sorted_hourly_traffic:
            self.logger.info(f"  - Heure {hour:02d}: {count} logs")

        self.logger.info("\nTrafic par localisation:")
        for location, count in self.realtime_aggregations['location_traffic'].items():
            self.logger.info(f"  - {location}: {count}")

        self.logger.info("\nTrafic par type d'appareil:")
        for device_type, count in self.realtime_aggregations['device_type_traffic'].items():
            self.logger.info(f"  - {device_type}: {count}")

        self.logger.info("\nTrafic par OS:")
        for os_name, count in self.realtime_aggregations['os_traffic'].items():
            self.logger.info(f"  - {os_name}: {count}")

        self.logger.info("\nRésumé des achats:")
        self.logger.info(f"  - Nombre total d'achats: {self.realtime_aggregations['purchase_summary']['count']}")
        self.logger.info(f"  - Montant total des achats: {self.realtime_aggregations['purchase_summary']['total_amount']:.2f}")

        self.logger.info("\nCompteurs d'erreurs par code:")
        for error_code, count in self.realtime_aggregations['error_counts'].items():
            self.logger.info(f"  - {error_code}: {count}")

        if self.realtime_aggregations['session_duration_count'] > 0:
            avg_duration = self.realtime_aggregations['session_duration_sum'] / self.realtime_aggregations['session_duration_count']
            self.logger.info(f"\nDurée moyenne des sessions: {avg_duration:.2f} secondes")
        else:
            self.logger.info("\nDurée moyenne des sessions: N/A (pas de sessions terminées)")

        self.logger.info("----------------------------------\n")


# --- Exécution du pipeline ---
if __name__ == "__main__":
    # Assurez-vous que app.py est en cours d'exécution pour générer des logs
    # ou exécutez-le dans un autre terminal
    # Exemple: python app.py

    pipeline = LogPipeline(log_file_path='app.log')
    try:
        # Le pipeline s'exécutera indéfiniment, en attendant de nouvelles lignes
        # Vous devrez l'arrêter manuellement (Ctrl+C)
        pipeline.process_logs_streaming(interval_seconds=0.5) # Vérifie le fichier toutes les 0.5 secondes
    except KeyboardInterrupt:
        pipeline.end_time = datetime.now()
        pipeline.logger.info("Pipeline arrêté par l'utilisateur.")
        pipeline.display_performance_metrics()
        pipeline.display_realtime_aggregations()
        parser_stats = pipeline.parser.get_stats()
        pipeline.logger.info(f"Statistiques finales du parser: Parsed={parser_stats['parsed']}, Failed={parser_stats['failed']}")
        if pipeline.parser.get_failed_logs():
            pipeline.logger.info("Logs échoués:")
            for failed_log in pipeline.parser.get_failed_logs():
                pipeline.logger.info(f"  - Reason: {failed_log['reason']}, Line: {failed_log['original_line'][:100]}...")

