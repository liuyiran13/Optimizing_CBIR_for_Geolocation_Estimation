package MyAPI.General.Magic;

import java.util.List;
import java.util.Set;

import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

public class ConnectComponent <K extends Object> {

	SimpleGraph<K, DefaultEdge> graph;
	
	public ConnectComponent() {
		graph = new SimpleGraph<K, DefaultEdge>(DefaultEdge.class);
	}
	
	public void addOneVertex(K one){
		graph.addVertex(one);
	}
	
	public void addOneEdge(K a, K b){
		graph.addEdge(a, b);
	}
	
	public List<Set<K>> getConnectComps(){
		ConnectivityInspector<K, DefaultEdge> inspector = new ConnectivityInspector<K, DefaultEdge>(graph);
		return inspector.connectedSets();
	}

}
