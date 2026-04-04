"""
ui/guided_tour.py  ·  Tour interactivo con overlay oscuro y highlights
======================================================================
Capa oscura fullscreen con cuadros de diálogo, flechas y highlights
que guía al usuario a través de la interfaz paso a paso.
"""

from PyQt6.QtWidgets import QWidget, QLabel, QVBoxLayout, QApplication
from PyQt6.QtCore import Qt, QTimer, QRect, QPoint, pyqtSignal
from PyQt6.QtGui import QColor, QPainter, QFont, QPen, QBrush, QPolygon
import math


class GuidedTourOverlay(QWidget):
    """
    Overlay fullscreen que muestra una capa oscura con highlights de widgets,
    cuadros de diálogo, y flechas animadas para guiar al usuario.
    """
    
    tour_finished = pyqtSignal()
    
    def __init__(self, parent=None):
        # No usar parent para que sea una ventana independiente
        super().__init__()
        # Flags para ventana sin bordes, siempre visible, y modal
        flags = (Qt.WindowType.FramelessWindowHint | 
                Qt.WindowType.WindowStaysOnTopHint | 
                Qt.WindowType.Tool)
        self.setWindowFlags(flags)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.parent_window = parent  # Guardar referencia al parent pero no como QWidget parent
        
        self.current_step = 0
        self.target_widgets = []  # Lista de (QWidget, titulo, descripción)
        self.anim_progress = 0
        
        # Timer para animación de flechas
        self.anim_timer = QTimer()
        self.anim_timer.timeout.connect(lambda: self._update_animation())
        
    def set_tour_steps(self, steps):
        """
        Configura los pasos del tour.
        steps: lista de dicts con:
            - widget: QWidget a destacar
            - title: str - título del paso
            - description: str - descripción del paso
            - arrow_from: str - dirección del cuadro de diálogo ('top', 'bottom', 'left', 'right')
        """
        self.target_widgets = steps
        
    def mousePressEvent(self, event):
        """Click para avanzar al siguiente paso."""
        self._next_step()
        
    def keyPressEvent(self, event):
        """ESC para cerrar, ENTER para siguiente paso."""
        if event.key() == Qt.Key.Key_Escape:
            self._close_tour()
        elif event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Space):
            self._next_step()
            
    def _next_step(self):
        """Avanzar al siguiente paso."""
        self.current_step += 1
        if self.current_step >= len(self.target_widgets):
            self._close_tour()
            return
        self.anim_progress = 0
        self.update()
        
    def _close_tour(self):
        """Cerrar el tour."""
        self.anim_timer.stop()
        self.hide()
        self.tour_finished.emit()
        
    def _update_animation(self):
        """Actualizar animación de la flecha."""
        self.anim_progress = (self.anim_progress + 0.05) % 1.0
        self.update()
        
    def start_tour(self):
        """Iniciar el tour."""
        if not self.target_widgets:
            return
        self.current_step = 0
        self.anim_progress = 0
        
        # Asegurar que cubre toda la pantalla
        screen = QApplication.primaryScreen()
        screen_geom = screen.geometry()
        self.setGeometry(screen_geom)
        
        self.showFullScreen()
        self.raise_()
        self.setFocus()
        self.anim_timer.start(50)
        self.update()
        
    def paintEvent(self, event):
        """Dibujar overlay oscuro, highlight, cuadro de diálogo y flecha."""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # 1. Fondo oscuro semi-transparente
        painter.fillRect(self.rect(), QColor(0, 0, 0, 200))
        
        if self.current_step >= len(self.target_widgets):
            painter.end()
            return
            
        step = self.target_widgets[self.current_step]
        widget = step.get('widget')
        title = step.get('title', '')
        description = step.get('description', '')
        arrow_from = step.get('arrow_from', 'bottom')
        
        if not widget or not widget.isVisible():
            painter.end()
            return
        
        # 2. Obtener geometría del widget (convertir de global a local del overlay)
        widget_global_pos = widget.mapToGlobal(QPoint(0, 0))
        widget_local_pos = self.mapFromGlobal(widget_global_pos)
        highlight_rect = QRect(widget_local_pos, widget.size())
        
        # Expandir el highlight un poco
        margin = 8
        highlight_rect.adjust(-margin, -margin, margin, margin)
        
        # 3. Dibujar el "hole" (transparente) alrededor del widget
        painter.setCompositionMode(QPainter.CompositionMode.CompositionMode_DestinationOut)
        painter.fillRect(highlight_rect, QColor(0, 0, 0, 255))
        painter.setCompositionMode(QPainter.CompositionMode.CompositionMode_SourceOver)
        
        # 4. Dibujar borde brillante alrededor del highlight
        pen = QPen(QColor(66, 165, 245, 220))
        pen.setWidth(3)
        painter.setPen(pen)
        painter.drawRect(highlight_rect)
        
        # 5. Calcular posición del cuadro de diálogo
        dialog_width = 320
        dialog_height = 140
        padding = 20
        
        # Determinar dónde colocar el diálogo basado en la dirección de la flecha
        if arrow_from == 'bottom':
            dialog_x = highlight_rect.center().x() - dialog_width // 2
            dialog_y = highlight_rect.bottom() + padding
        elif arrow_from == 'top':
            dialog_x = highlight_rect.center().x() - dialog_width // 2
            dialog_y = highlight_rect.top() - dialog_height - padding
        elif arrow_from == 'left':
            dialog_x = highlight_rect.left() - dialog_width - padding
            dialog_y = highlight_rect.center().y() - dialog_height // 2
        else:  # right
            dialog_x = highlight_rect.right() + padding
            dialog_y = highlight_rect.center().y() - dialog_height // 2
            
        # Asegurar que el diálogo esté dentro de los límites
        dialog_x = max(10, min(dialog_x, self.width() - dialog_width - 10))
        dialog_y = max(10, min(dialog_y, self.height() - dialog_height - 10))
        
        dialog_rect = QRect(dialog_x, dialog_y, dialog_width, dialog_height)
        
        # 6. Dibujar cuadro de diálogo
        painter.setCompositionMode(QPainter.CompositionMode.CompositionMode_SourceOver)
        painter.fillRect(dialog_rect, QColor(33, 33, 33, 240))
        
        # Borde del diálogo
        dialog_pen = QPen(QColor(66, 165, 245, 220))
        dialog_pen.setWidth(2)
        painter.setPen(dialog_pen)
        painter.drawRect(dialog_rect)
        
        # 7. Dibujar texto del diálogo
        text_rect = dialog_rect.adjusted(12, 12, -12, -12)
        
        # Título
        title_font = QFont()
        title_font.setPointSize(11)
        title_font.setBold(True)
        painter.setFont(title_font)
        painter.setPen(QColor(255, 255, 255, 255))
        painter.drawText(text_rect.adjusted(0, 0, 0, -60), Qt.TextFlag.TextWordWrap, title)
        
        # Descripción
        desc_font = QFont()
        desc_font.setPointSize(9)
        painter.setFont(desc_font)
        painter.setPen(QColor(200, 200, 200, 230))
        desc_rect = text_rect.adjusted(0, 50, 0, 0)
        painter.drawText(desc_rect, Qt.TextFlag.TextWordWrap, description)
        
        # 8. Dibujar efecto de pulso sutilizado alrededor del diálogo
        pulse_radius = int(4 + 3 * math.sin(self.anim_progress * 3.14159 * 2))
        pulse_color = QColor(66, 165, 245, int(80 - 30 * abs(self.anim_progress - 0.5)))
        pulse_pen = QPen(pulse_color)
        pulse_pen.setWidth(2)
        painter.setPen(pulse_pen)
        painter.drawRect(dialog_rect.adjusted(-pulse_radius, -pulse_radius, pulse_radius, pulse_radius))
        
        # 9. Dibujar pie de página con instrucciones
        footer_text = f"Paso {self.current_step + 1} de {len(self.target_widgets)}  ·  Click para continuar  ·  ESC para salir"
        footer_font = QFont()
        footer_font.setPointSize(8)
        painter.setFont(footer_font)
        painter.setPen(QColor(150, 150, 150, 200))
        painter.drawText(
            self.rect().adjusted(20, 0, -20, 30),
            Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignBottom,
            footer_text
        )
        
        painter.end()
