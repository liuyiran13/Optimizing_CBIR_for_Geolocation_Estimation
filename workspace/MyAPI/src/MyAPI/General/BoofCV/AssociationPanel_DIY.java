/*
 * Copyright (c) 2011-2012, Peter Abeles. All Rights Reserved.
 *
 * This file is part of BoofCV (http://boofcv.org).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package MyAPI.General.BoofCV;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;
import MyCustomedHaoop.ValueClass.SURFpoint;
import boofcv.gui.feature.VisualizeFeatures;

import java.awt.*;
import java.awt.event.MouseListener;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * Shows which two features are associated with each other.  An individual feature
 * can be shown alone by clicking on it.
 *
 */
@SuppressWarnings("serial")
public class AssociationPanel_DIY extends CompareTwoImagePanel_DIY implements MouseListener {
	
	protected class Ind_Color{
		private int ind;
		private Color color;
		
		public Ind_Color(int ind, Color color) {
			this.ind=ind;
			this.color=color;
		}
		
		public int getInd(){
			return ind;
		}
		
		public Color getColor(){
			return color;
		}
	}
	
	// which features are associated with each other
	private ArrayList<ArrayList<Ind_Color>> assocLeft,assocRight; //for: one point has multiple matches
		
	public AssociationPanel_DIY(int borderSize, int pointEnlargeFactor) {
		super(borderSize, true, pointEnlargeFactor);		
	}

	public synchronized void setAssociation( List<SURFpoint> leftPts , List<SURFpoint> rightPts,
											 List<ImageRegionMatch> matches, int RGBInd, double[] scalingInfo_matchStrength ) {
		setLocation(leftPts,rightPts);
		assocLeft = General.ini_ArrayList_ArrayList(leftPts.size(),10);
		assocRight = General.ini_ArrayList_ArrayList(rightPts.size(),10);
		
		if (RGBInd<0) {//rand color for each match
			Random rand = new Random(9);
			for(ImageRegionMatch a: matches ) {
				Color color=new Color(rand.nextInt() | 0xFF000000 );
				assocLeft.get(a.src).add(new Ind_Color(a.dst, color) );
				assocRight.get(a.dst).add(new Ind_Color(a.src, color));
			}
		}else if (RGBInd>2) {//use one color, ignore strength
			for(ImageRegionMatch a: matches ) {
				Color color=new Color(200,200,200);
				assocLeft.get(a.src).add(new Ind_Color(a.dst, color) );
				assocRight.get(a.dst).add(new Ind_Color(a.src, color));
			}
		}else {//draw color according to match strength, RGBInd indicate red, green or blue
			//make association
			for(ImageRegionMatch a: matches ) {
				Color color=General.getStrengthColor(RGBInd, a.matchScore, scalingInfo_matchStrength);//normalize match strength to 0~1.0
				assocLeft.get(a.src).add(new Ind_Color(a.dst, color) );
				assocRight.get(a.dst).add(new Ind_Color(a.src, color));
			}
		}
		
	}

//	public synchronized void setAssociation( List<AssociatedPair> matches ) {
//		List<SURFpoint> leftPts = new ArrayList<SURFpoint>();
//		List<SURFpoint> rightPts = new ArrayList<SURFpoint>();
//
//		for( AssociatedPair p : matches ) {
//			leftPts.add(p.keyLoc);
//			rightPts.add(p.currLoc);
//		}
//
//		setLocation(leftPts,rightPts);
//
//		assocLeft = new int[ leftPts.size() ];
//		assocRight = new int[ rightPts.size() ];
//
//		for( int i = 0; i < assocLeft.length; i++ ) {
//			assocLeft[i] = i;
//			assocRight[i] = i;
//		}
//
//		Random rand = new Random(234);
//		colors = new Color[ leftPts.size() ];
//		for( int i = 0; i < colors.length; i++ ) {
//			colors[i] = new Color(rand.nextInt() | 0xFF000000 );
//		}
//	}

	@Override
	protected void drawFeatures(Graphics2D g2 ,
							 double scaleLeft, int leftX, int leftY,
							 double scaleRight, int rightX, int rightY) {
		if( selected.isEmpty() )
			drawAllFeatures(g2, scaleLeft,scaleRight,rightX);
		else {

			for( int selectedIndex : selected ) {
				// draw just an individual feature pair
				SURFpoint l,r;
				Color color;

				if( selectedIsLeft ) {
					l = leftPts.get(selectedIndex);
					ArrayList<Ind_Color> l_de_matches=assocLeft.get(selectedIndex);
					if( l_de_matches.size()==0 ) {//left point do not have any matches
						drawAllFeatures(g2, scaleLeft,scaleRight,rightX);
					} else {
						for (int i = 0; i < l_de_matches.size(); i++) {
							Ind_Color oneMatch=l_de_matches.get(i);
							r = rightPts.get(oneMatch.getInd());
							color = oneMatch.getColor();
							drawAssociation_fullPointInfo(g2, scaleLeft,scaleRight,rightX, l, r, color);
						}
					}
				} else {
					r = rightPts.get(selectedIndex);
					ArrayList<Ind_Color> r_de_matches=assocRight.get(selectedIndex);
					if( r_de_matches.size()==0 ) {//left point do not have any matches
						drawAllFeatures(g2, scaleLeft,scaleRight,rightX);
					} else {
						for (int i = 0; i < r_de_matches.size(); i++) {
							Ind_Color oneMatch=r_de_matches.get(i);
							l = leftPts.get(oneMatch.getInd());
							color = oneMatch.getColor();
							drawAssociation_fullPointInfo(g2, scaleLeft,scaleRight,rightX, l, r, color);
						}
					}
				}
			}
		}
	}

	private void drawAllFeatures(Graphics2D g2, double scaleLeft , double scaleRight , int rightX) {
		if( assocLeft == null || rightPts == null || leftPts == null )
			return;

		for( int i = 0; i < assocLeft.size(); i++ ) {
			ArrayList<Ind_Color> l_de_matches=assocLeft.get(i);
			SURFpoint l = leftPts.get(i);
			for (Ind_Color oneMatch: l_de_matches) {
				SURFpoint r = rightPts.get(oneMatch.getInd());
				Color color = oneMatch.getColor();
				drawAssociation_fullPointInfo(g2, scaleLeft,scaleRight,rightX, l, r, color);
			}
		}
	}

	@SuppressWarnings("unused")
	private void drawAssociation(Graphics2D g2, double scaleLeft , double scaleRight , int rightX, SURFpoint l, SURFpoint r, Color color) {
		if( r == null ) {
			int x1 = (int)(scaleLeft*l.x);
			int y1 = (int)(scaleLeft*l.y);
			VisualizeFeatures.drawPoint(g2,x1,y1,Color.RED);
		} else if( l == null ) {
			int x2 = (int)(scaleRight*r.x) + rightX;
			int y2 = (int)(scaleRight*r.y);
			VisualizeFeatures.drawPoint(g2,x2,y2,Color.RED);
		} else {
			int x1 = (int)(scaleLeft*l.x);
			int y1 = (int)(scaleLeft*l.y);
			VisualizeFeatures.drawPoint(g2,x1,y1,color);

			int x2 = (int)(scaleRight*r.x) + rightX;
			int y2 = (int)(scaleRight*r.y);
			VisualizeFeatures.drawPoint(g2,x2,y2,color);

			g2.setColor(color);
			g2.drawLine(x1,y1,x2,y2);
		}
	}
	
	private void drawAssociation_fullPointInfo(Graphics2D g2, double scaleLeft , double scaleRight , int rightX, SURFpoint l, SURFpoint r, Color color) {
		if( r == null ) {
			General_BoofCV.drawInterestPoint(g2, l, scaleLeft, 0, 0, color, pointEnlargeFactor, true);
		} else if( l == null ) {
			General_BoofCV.drawInterestPoint(g2, r, scaleRight, rightX, 0, color, pointEnlargeFactor, true);
		} else {			
			int[] pointInPlan_l=General_BoofCV.drawInterestPoint(g2, l, scaleLeft, 0, 0, color, pointEnlargeFactor, true);
			int[] pointInPlan_r=General_BoofCV.drawInterestPoint(g2, r, scaleRight, rightX, 0, color, pointEnlargeFactor, true);
			//draw linked points
			g2.setColor(color);
			g2.drawLine(pointInPlan_l[0], pointInPlan_l[1], pointInPlan_r[0], pointInPlan_r[1]);
		}
	}

	@Override
	protected boolean isValidPoint(int index) {
		if( selectedIsLeft )
			return assocLeft.size() >0;
		else
			return assocRight.size() >0;
	}
}
