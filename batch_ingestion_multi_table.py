"""
Batch Ingestion Script for Multi-Table Kusto Data

This script generates 30 minutes of timestamped data for 3 tables and ingests them in batches.
Perfect for scheduled runs every 30 minutes to create a live-looking dashboard.

Mode: Forward-looking - generates data for the next 30 minutes (now to now + 30 minutes)
"""

import os
import time
import random
import math
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

# Load environment variables
load_dotenv()

class BatchIngestorMultiTable:
    """Batch ingestion for 3 tables with 30 minutes of timestamped data"""
    
    def __init__(self, batch_duration_minutes=30, batch_number=None):
        self.cluster_url = os.getenv('CLUSTER_URL')
        self.database_name = os.getenv('DATABASE_NAME')
        self.batch_duration_minutes = batch_duration_minutes
        
        # Batch tracking for anomaly detection
        self.batch_number = batch_number or self._calculate_batch_number()
        self.is_anomaly_batch = (self.batch_number % 6 == 0)  # Every 6th batch
        
        # Table names
        self.tables = ['HttpIncoming', 'HttpOutgoing', 'SLL']
        
        # Statistics tracking
        self.stats = {
            'HttpIncoming': {'generated': 0, 'success': 0, 'failed': 0},
            'HttpOutgoing': {'generated': 0, 'success': 0, 'failed': 0},
            'SLL': {'generated': 0, 'success': 0, 'failed': 0}
        }
        
        print("🚀 Batch Multi-Table Kusto Ingestion")
        print("=" * 70)
        print(f"🔧 Target: {self.cluster_url}")
        print(f"📊 Database: {self.database_name}")
        print(f"📋 Tables: {', '.join(self.tables)}")
        print(f"⏱️  Batch Duration: {self.batch_duration_minutes} minutes")
        print(f"🔢 Batch Number: #{self.batch_number}")
        
        if self.is_anomaly_batch:
            print(f"🚨 ANOMALY BATCH: This batch will contain unusual patterns!")
            print(f"⚡ Method: Forward-looking with ANOMALOUS data patterns")
        else:
            print(f"⚡ Method: Forward-looking with realistic timestamps")
        print("-" * 70)
        
        # Calculate time range for this batch
        self._calculate_time_range()
        
        # Setup anomaly patterns if this is an anomaly batch
        self._setup_anomaly_patterns()
        
        # Data generation ranges and values
        self._setup_data_ranges()
        
        # Setup connection
        self._setup_connection()
    
    def _calculate_batch_number(self):
        """Calculate batch number based on current time"""
        # Use current time to determine batch number
        # This ensures consistency across runs
        now = datetime.now(timezone.utc)
        # Calculate total 30-minute periods since epoch
        epoch = datetime(2025, 1, 1, tzinfo=timezone.utc)
        total_minutes = int((now - epoch).total_seconds() / 60)
        batch_number = (total_minutes // 30) + 1
        return batch_number
    
    def _calculate_time_range(self):
        """Calculate the time range for this batch - generates data for the next 30 minutes"""
        # Forward-looking: start time is now, end time is 30 minutes from now
        now = datetime.now(timezone.utc)
        
        # Start time is current time
        self.batch_start_time = now
        
        # End time is 30 minutes from now
        self.batch_end_time = now + timedelta(minutes=self.batch_duration_minutes)
        
        # Calculate total seconds and records per table
        self.total_seconds = self.batch_duration_minutes * 60
        self.records_per_table = self.total_seconds  # 1 record per second per table
        
        print(f"📅 Batch Time Range:")
        print(f"   Start: {self.batch_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"   End:   {self.batch_end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"   Duration: {self.batch_duration_minutes} minutes ({self.total_seconds} seconds)")
        print(f"   Records per table: {self.records_per_table}")
        print(f"   Total records: {self.records_per_table * len(self.tables)}")
        print()
    
    def _setup_data_ranges(self):
        """Setup data generation ranges and values"""
        # HTTP Status codes with realistic weights
        if self.is_anomaly_batch and hasattr(self, 'ddos_status_codes'):
            # For DDoS attacks, weight towards error codes
            self.http_status_codes = [200, 203, 204] + self.ddos_status_codes
            self.http_weights = [20, 5, 5, 25, 20, 15, 10]  # More errors
        else:
            self.http_status_codes = [200, 203, 204, 400, 404, 500, 503]
            self.http_weights = [50, 10, 5, 15, 10, 5, 5]  # 200 is most common
        
        # Event names for SLL table
        self.event_names = [
            'UserLogin', 'DatabaseQuery', 'FileAccess', 'NetworkConnection',
            'SystemStartup', 'ProcessCreated', 'ServiceStarted', 'ConfigChanged',
            'SecurityScan', 'BackupCompleted', 'UpdateInstalled', 'ErrorOccurred'
        ]
        
        # Severity levels with realistic distribution
        if self.is_anomaly_batch:
            if self.anomaly_type in ['system_failure', 'security_incident']:
                self.severity_levels = ['INFO', 'WARNING', 'ERROR']
                self.severity_weights = [30, 40, 30]  # More warnings/errors
            elif self.anomaly_type == 'maintenance_mode':
                self.severity_levels = ['INFO', 'WARNING', 'ERROR']
                self.severity_weights = [90, 8, 2]  # Mostly INFO
            else:
                self.severity_levels = ['INFO', 'WARNING', 'ERROR']
                self.severity_weights = [50, 30, 20]  # Moderate increase in issues
        else:
            self.severity_levels = ['INFO', 'WARNING', 'ERROR']
            self.severity_weights = [70, 20, 10]  # INFO is most common
        
        print("📋 Data Generation Setup:")
        print(f"   Count Range: 100-1000{' (with anomaly multiplier)' if self.is_anomaly_batch else ''}")
        print(f"   HTTP Status Codes: {self.http_status_codes} (weighted)")
        print(f"   Event Names: {len(self.event_names)} different events")
        print(f"   Severity Levels: {self.severity_levels} (weighted)")
        if self.is_anomaly_batch:
            print(f"   🚨 Anomaly Multiplier: {getattr(self, 'anomaly_multiplier', 1.0):.1f}x")
            print(f"   🚨 Error Rate: {getattr(self, 'anomaly_error_rate', 0.1):.1%}")
        print()
    
    def _setup_connection(self):
        """Setup Kusto connection"""
        try:
            client_id = os.getenv('CLIENT_ID')
            client_secret = os.getenv('CLIENT_SECRET')
            tenant_id = os.getenv('TENANT_ID')
            
            print("🔐 Establishing connection...")
            self.kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                self.cluster_url, client_id, client_secret, tenant_id
            )
            
            self.kusto_client = KustoClient(self.kcsb)
            print("✅ Connection established!")
            print()
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            raise
    
    def generate_timestamp_sequence(self):
        """Generate a sequence of timestamps for the batch period"""
        timestamps = []
        current_time = self.batch_start_time
        
        for i in range(self.total_seconds):
            timestamps.append(current_time + timedelta(seconds=i))
        
        return timestamps
    
    def generate_http_data_batch(self, table_name, timestamps):
        """Generate a batch of HTTP data (for HttpIncoming or HttpOutgoing)"""
        data_batch = []
        
        for timestamp in timestamps:
            # Add some variation based on time of day for realism
            hour = timestamp.hour
            
            # Vary request counts based on "business hours"
            if 9 <= hour <= 17:  # Business hours - higher activity
                count_min, count_max = 300, 1000
            elif 6 <= hour <= 9 or 17 <= hour <= 22:  # Peak hours - medium activity
                count_min, count_max = 150, 600
            else:  # Night hours - lower activity
                count_min, count_max = 100, 300
            
            base_count = random.randint(count_min, count_max)
            base_status = random.choices(self.http_status_codes, weights=self.http_weights)[0]
            
            # Apply anomaly patterns if applicable
            if self.is_anomaly_batch:
                anomaly_count, anomaly_status = self._apply_anomaly_to_http_data(base_count, base_status)
            else:
                anomaly_count, anomaly_status = base_count, base_status
            
            data_batch.append({
                'CountRequest': anomaly_count,
                'HttpStatusCode': anomaly_status,
                'Time': timestamp
            })
        
        return data_batch
    
    def generate_sll_data_batch(self, timestamps):
        """Generate a batch of SLL data"""
        data_batch = []
        
        for timestamp in timestamps:
            # Vary event frequency based on time (more errors during business hours)
            hour = timestamp.hour
            
            if 9 <= hour <= 17:  # Business hours - more diverse events
                count_min, count_max = 200, 1000
                severity_weights = [60, 25, 15]  # More warnings/errors during business hours
            else:  # Off hours - mostly INFO events
                count_min, count_max = 100, 400
                severity_weights = [85, 10, 5]  # Mostly INFO during off hours
            
            base_count = random.randint(count_min, count_max)
            base_event = random.choice(self.event_names)
            base_severity = random.choices(self.severity_levels, weights=severity_weights)[0]
            
            # Apply anomaly patterns if applicable
            if self.is_anomaly_batch:
                anomaly_count, anomaly_event, anomaly_severity = self._apply_anomaly_to_sll_data(base_count, base_event, base_severity)
            else:
                anomaly_count, anomaly_event, anomaly_severity = base_count, base_event, base_severity
            
            data_batch.append({
                'Count': anomaly_count,
                'Eventname': anomaly_event,
                'Severity': anomaly_severity,
                'Time': timestamp
            })
        
        return data_batch
    
    def create_batch_kql_command(self, table_name, data_batch):
        """Create a batch KQL command for multiple records"""
        if table_name in ['HttpIncoming', 'HttpOutgoing']:
            # Create datatable with all records
            records = []
            for data in data_batch:
                time_str = data['Time'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                records.append(f"    {data['CountRequest']}, {data['HttpStatusCode']}, datetime({time_str})")
            
            records_str = ",\n".join(records)
            
            kql_command = f"""
            .set-or-append {table_name} <|
            datatable(CountRequest:long, HttpStatusCode:long, Time:datetime) [
{records_str}
            ]
            """
        
        elif table_name == 'SLL':
            # Create datatable with all records
            records = []
            for data in data_batch:
                time_str = data['Time'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                records.append(f'    {data["Count"]}, "{data["Eventname"]}", "{data["Severity"]}", datetime({time_str})')
            
            records_str = ",\n".join(records)
            
            kql_command = f"""
            .set-or-append {table_name} <|
            datatable(Count:long, Eventname:string, Severity:string, Time:datetime) [
{records_str}
            ]
            """
        
        return kql_command
    
    def ingest_batch_to_table(self, table_name, data_batch, batch_size=100):
        """Ingest a batch of data to a table in smaller chunks"""
        total_records = len(data_batch)
        successful_records = 0
        failed_records = 0
        
        print(f"📤 Ingesting {total_records} records to {table_name} in chunks of {batch_size}...")
        
        # Split into smaller batches to avoid KQL size limits
        for i in range(0, total_records, batch_size):
            chunk = data_batch[i:i + batch_size]
            chunk_size = len(chunk)
            
            try:
                kql_command = self.create_batch_kql_command(table_name, chunk)
                self.kusto_client.execute_mgmt(self.database_name, kql_command)
                
                successful_records += chunk_size
                print(f"   ✅ Chunk {i//batch_size + 1}: {chunk_size} records ingested")
                
                # Small delay between chunks to avoid overwhelming Kusto
                time.sleep(0.5)
                
            except Exception as e:
                failed_records += chunk_size
                print(f"   ❌ Chunk {i//batch_size + 1}: Failed to ingest {chunk_size} records - {e}")
        
        # Update statistics
        self.stats[table_name]['generated'] = total_records
        self.stats[table_name]['success'] = successful_records
        self.stats[table_name]['failed'] = failed_records
        
        success_rate = (successful_records / total_records * 100) if total_records > 0 else 0
        print(f"   📊 {table_name}: {successful_records}/{total_records} successful ({success_rate:.1f}%)")
        
        return successful_records, failed_records
    
    def run_batch_ingestion(self):
        """Run the complete batch ingestion process"""
        print("🔄 Starting batch ingestion process...")
        print("="*70)
        
        start_time = time.time()
        
        # Generate timestamp sequence
        print("📅 Generating timestamp sequence...")
        timestamps = self.generate_timestamp_sequence()
        print(f"   ✅ Generated {len(timestamps)} timestamps")
        print()
        
        # Process each table
        for table_name in self.tables:
            print(f"🏗️  Processing {table_name}:")
            
            # Generate data batch
            if table_name in ['HttpIncoming', 'HttpOutgoing']:
                data_batch = self.generate_http_data_batch(table_name, timestamps)
            elif table_name == 'SLL':
                data_batch = self.generate_sll_data_batch(timestamps)
            
            print(f"   📋 Generated {len(data_batch)} records")
            
            # Ingest batch
            success_count, failed_count = self.ingest_batch_to_table(table_name, data_batch)
            
            print(f"   ✅ Completed {table_name}: {success_count} success, {failed_count} failed")
            print()
        
        # Final summary
        elapsed_time = time.time() - start_time
        self._print_final_summary(elapsed_time)
    
    def _print_final_summary(self, elapsed_time):
        """Print final ingestion summary"""
        print("="*70)
        if self.is_anomaly_batch:
            print("📊 ANOMALY BATCH INGESTION SUMMARY")
            print(f"🚨 Anomaly Type: {self.anomaly_type}")
        else:
            print("📊 BATCH INGESTION SUMMARY")
        print("="*70)
        
        total_generated = sum(self.stats[table]['generated'] for table in self.stats)
        total_success = sum(self.stats[table]['success'] for table in self.stats)
        total_failed = sum(self.stats[table]['failed'] for table in self.stats)
        
        print(f"🔢 Batch Number: #{self.batch_number}")
        print(f"⏱️  Execution Time: {elapsed_time:.1f} seconds")
        print(f"📈 Total Records Generated: {total_generated}")
        print(f"✅ Successfully Ingested: {total_success}")
        print(f"❌ Failed Ingestions: {total_failed}")
        
        if total_generated > 0:
            overall_success_rate = total_success / total_generated * 100
            print(f"🎯 Overall Success Rate: {overall_success_rate:.1f}%")
        
        print(f"\n📋 Per-Table Results:")
        for table in self.tables:
            stats = self.stats[table]
            success_rate = (stats['success'] / stats['generated'] * 100) if stats['generated'] > 0 else 0
            print(f"   {table}: {stats['success']}/{stats['generated']} ({success_rate:.1f}%)")
        
        print(f"\n📅 Data Time Range:")
        print(f"   {self.batch_start_time.strftime('%Y-%m-%d %H:%M:%S')} to {self.batch_end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        if self.is_anomaly_batch:
            print(f"\n🚨 Anomaly Details:")
            print(f"   Type: {self.anomaly_type}")
            print(f"   Traffic Multiplier: {getattr(self, 'anomaly_multiplier', 1.0):.1f}x")
            print(f"   Error Rate: {getattr(self, 'anomaly_error_rate', 0.1):.1%}")
            print(f"   Next anomaly batch: #{self.batch_number + 6}")
        else:
            next_anomaly = ((self.batch_number // 6) + 1) * 6
            print(f"\n📊 Normal Batch - Next anomaly batch: #{next_anomaly}")
        
        print(f"\n💾 Data should appear in Kusto within 5-10 minutes")
        print(f"🎯 Check your tables: {', '.join(self.tables)}")
        print(f"\n🎉 Batch ingestion completed!")

    def _setup_anomaly_patterns(self):
        """Setup anomaly patterns for unusual batches"""
        if not self.is_anomaly_batch:
            return
            
        print("🚨 Anomaly Pattern Configuration:")
        
        # Choose a random anomaly type
        anomaly_types = [
            'traffic_spike',     # Massive increase in requests
            'system_failure',    # High error rates
            'security_incident', # Unusual patterns + errors
            'maintenance_mode',  # Very low activity
            'ddos_attack'        # Specific attack patterns
        ]
        
        self.anomaly_type = random.choice(anomaly_types)
        
        print(f"   🎯 Anomaly Type: {self.anomaly_type}")
        
        if self.anomaly_type == 'traffic_spike':
            print("   📈 Pattern: 5-10x normal traffic, mostly successful")
            self.anomaly_multiplier = random.uniform(5, 10)
            self.anomaly_error_rate = 0.15  # 15% errors due to load
            
        elif self.anomaly_type == 'system_failure':
            print("   💥 Pattern: High error rates (50-80%), normal traffic")
            self.anomaly_multiplier = random.uniform(0.8, 1.2)
            self.anomaly_error_rate = random.uniform(0.5, 0.8)
            
        elif self.anomaly_type == 'security_incident':
            print("   🔐 Pattern: Unusual event types, high WARNING/ERROR logs")
            self.anomaly_multiplier = random.uniform(1.5, 3.0)
            self.anomaly_error_rate = 0.3
            self.security_events = ['SecurityBreach', 'UnauthorizedAccess', 'MalwareDetected', 'SuspiciousActivity']
            
        elif self.anomaly_type == 'maintenance_mode':
            print("   🔧 Pattern: Very low activity, mostly INFO logs")
            self.anomaly_multiplier = random.uniform(0.1, 0.3)
            self.anomaly_error_rate = 0.05
            
        elif self.anomaly_type == 'ddos_attack':
            print("   ⚔️  Pattern: Massive traffic, high timeouts/errors")
            self.anomaly_multiplier = random.uniform(8, 15)
            self.anomaly_error_rate = 0.6
            self.ddos_status_codes = [503, 504, 429, 500]  # Service unavailable, timeouts
        
        print()
    
    def _apply_anomaly_to_http_data(self, base_count, base_status):
        """Apply anomaly patterns to HTTP data"""
        if not self.is_anomaly_batch:
            return base_count, base_status
            
        # Apply traffic multiplier
        anomaly_count = int(base_count * self.anomaly_multiplier)
        
        # Apply error patterns
        if random.random() < self.anomaly_error_rate:
            if self.anomaly_type == 'ddos_attack':
                anomaly_status = random.choice(self.ddos_status_codes)
            elif self.anomaly_type == 'system_failure':
                anomaly_status = random.choice([500, 503, 502, 504])
            else:
                anomaly_status = random.choice([400, 404, 500, 503])
        else:
            anomaly_status = base_status
            
        return anomaly_count, anomaly_status
    
    def _apply_anomaly_to_sll_data(self, base_count, base_event, base_severity):
        """Apply anomaly patterns to SLL data"""
        if not self.is_anomaly_batch:
            return base_count, base_event, base_severity
            
        # Apply traffic multiplier
        anomaly_count = int(base_count * self.anomaly_multiplier)
        
        # Apply event and severity patterns
        if self.anomaly_type == 'security_incident':
            if random.random() < 0.4:  # 40% chance of security events
                anomaly_event = random.choice(self.security_events)
            else:
                anomaly_event = base_event
            
            # More warnings and errors during security incidents
            if random.random() < 0.7:
                anomaly_severity = random.choice(['WARNING', 'ERROR'])
            else:
                anomaly_severity = 'INFO'
                
        elif self.anomaly_type == 'system_failure':
            # System failure events
            failure_events = ['SystemCrash', 'ServiceTimeout', 'DatabaseError', 'MemoryLeak', 'DiskFull']
            if random.random() < 0.5:
                anomaly_event = random.choice(failure_events)
            else:
                anomaly_event = base_event
            
            # Mostly errors and warnings
            if random.random() < 0.8:
                anomaly_severity = random.choice(['WARNING', 'ERROR'])
            else:
                anomaly_severity = 'INFO'
                
        elif self.anomaly_type == 'maintenance_mode':
            # Maintenance events
            maintenance_events = ['MaintenanceStart', 'ServiceRestart', 'ConfigUpdate', 'BackupInProgress']
            if random.random() < 0.3:
                anomaly_event = random.choice(maintenance_events)
            else:
                anomaly_event = base_event
            
            # Mostly INFO during maintenance
            anomaly_severity = 'INFO' if random.random() < 0.9 else 'WARNING'
            
        else:
            # For traffic_spike and ddos_attack, keep similar events but change severity
            anomaly_event = base_event
            if random.random() < self.anomaly_error_rate:
                anomaly_severity = random.choice(['WARNING', 'ERROR'])
            else:
                anomaly_severity = base_severity
                
        return anomaly_count, anomaly_event, anomaly_severity
def main():
    """Main function"""
    print("🎯 BATCH MULTI-TABLE KUSTO INGESTION")
    print("Forward-looking mode: 30 minutes of future timestamped data")
    print()
    
    try:
        # Create and run batch ingestor
        ingestor = BatchIngestorMultiTable(batch_duration_minutes=30)
        ingestor.run_batch_ingestion()
        
    except Exception as e:
        print(f"❌ Batch ingestion failed: {e}")
        raise

if __name__ == "__main__":
    main()
