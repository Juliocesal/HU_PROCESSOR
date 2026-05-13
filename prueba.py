import pythoncom
import win32com.client

pythoncom.CoInitialize()

sap_gui = win32com.client.GetObject("SAPGUI")
app     = sap_gui.GetScriptingEngine

print(f"Conexiones activas: {app.Children.Count}")

for i_conn in range(int(app.Children.Count)):
    conn = app.Children(i_conn)
    print(f"\n── Conexión {i_conn} ──")
    print(f"   Sesiones en esta conexión: {conn.Children.Count}")

    for i_sess in range(int(conn.Children.Count)):
        sess = conn.Children(i_sess)
        print(f"   Sesión [{i_sess}] → Usuario: {sess.Info.User} | Sistema: {sess.Info.SystemName} | TX: {sess.Info.Transaction}")