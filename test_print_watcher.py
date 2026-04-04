"""
Script para diagnóstico de print_watcher
Ejecuta print_watcher y te muestra qué ventanas detecta
"""

import time
import logging
from core.print_watcher import PrintDialogWatcher

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

log = logging.getLogger(__name__)

if __name__ == "__main__":
    print("=" * 60)
    print("PRINT WATCHER DIAGNOSIS TOOL")
    print("=" * 60)
    
    watcher = PrintDialogWatcher(idle_timeout=15.0, poll_interval=0.2)
    
    print("\n1. Iniciando watcher...")
    watcher.start()
    
    print("2. Watcher activo por 15 segundos (o hasta idle timeout)")
    print("3. AHORA GENERA UNA VENTANA DE IMPRESIÓN EN SAP")
    print("   - Ve a SAP y ejecuta SP01")
    print("   - El watcher capturará la ventana y su información\n")
    
    # Esperar a que el watcher se ejecute
    try:
        while watcher.is_running():
            print(f"   ... Watcher running (confirmaciones: {watcher.confirmed_count})", end='\r')
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n   Cancelado por usuario")
    
    watcher.stop()
    
    print(f"\n\nRESULTADO: {watcher.confirmed_count} diálogos confirmados")
    print("\n" + "=" * 60)
    print("Revisa los logs arriba para ver qué se detectó ↑↑↑")
    print("=" * 60)
